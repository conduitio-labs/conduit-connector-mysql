// Copyright © 2026 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-sql-driver/mysql"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

// The tests in this file are regression tests for the #232 review blocker: the
// column re-read and the Table_map check ran on every event canal handed over,
// before the resume decided which of the events it reads again were delivered
// before. An ordinary restart after a DDL then halted with
// mysql.cdc.row_columns_mismatch on a row that was already delivered, and the
// rows written after the DDL were never delivered.

const ddlResumeTable = "ddl_resume"

// setBinlogRowMetadata sets the server's binlog_row_metadata for the test and
// restores the value it had before when the test ends.
func setBinlogRowMetadata(ctx context.Context, t *testing.T, is *is.I, value string) {
	t.Helper()
	db := testutils.NewDB(t).SqlxDB
	var prior string
	is.NoErr(db.QueryRowContext(ctx, "SELECT @@GLOBAL.binlog_row_metadata").Scan(&prior))
	_, err := db.ExecContext(ctx, "SET GLOBAL binlog_row_metadata = "+value)
	is.NoErr(err)
	t.Cleanup(func() {
		// ctx is cancelled by then; the restore must still run.
		_, err := db.ExecContext(context.WithoutCancel(ctx), "SET GLOBAL binlog_row_metadata = "+prior)
		is.NoErr(err)
	})
}

// ddlResumeIterator starts a CDC iterator on ddl_resume: from the server's
// current position when pos is nil, from the saved position otherwise.
func ddlResumeIterator(ctx context.Context, t *testing.T, is *is.I, pos opencdc.Position) (common.Iterator, func()) {
	t.Helper()
	config, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)
	cfg := cdcIteratorConfig{
		mysqlConfig:         config,
		tables:              []string{ddlResumeTable},
		tableKeys:           common.TableKeys{ddlResumeTable: {"id"}},
		db:                  testutils.NewDB(t).SqlxDB,
		disableCanalLogging: true,
	}
	if pos != nil {
		saved, err := common.ParseSDKPosition(pos)
		is.NoErr(err)
		is.True(saved.CdcPosition != nil)
		cfg.startPosition = saved.CdcPosition
	}
	iterator, err := newCdcIterator(ctx, cfg)
	is.NoErr(err)
	if pos == nil {
		is.NoErr(iterator.obtainStartPosition())
	}
	is.NoErr(iterator.start(ctx))
	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func createDDLResumeTable(exec sessionExec) {
	exec("DROP TABLE IF EXISTS " + ddlResumeTable)
	exec("CREATE TABLE " + ddlResumeTable + " (id BIGINT PRIMARY KEY, a INT, b INT)")
}

// forEachRowMetadata runs the test with the server's default binlog_row_metadata
// and with FULL, which adds the column names to the Table_map event.
func forEachRowMetadata(t *testing.T, test func(t *testing.T, ctx context.Context, is *is.I)) {
	t.Helper()
	for _, mode := range []string{"default", "FULL"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			if mode == "FULL" {
				setBinlogRowMetadata(ctx, t, is, "FULL")
			}
			test(t, ctx, is)
		})
	}
}

// Invariant 3: a column type changes while the connector is stopped, and rows
// are written after it. The restart reads the row it delivered before the stop
// again, under the old column type; it is dropped, not checked, and the rows
// after the ALTER are delivered.
func TestCDCIterator_RestartAfterTypeChangeDeliversLaterRows(t *testing.T) {
	forEachRowMetadata(t, func(t *testing.T, ctx context.Context, is *is.I) {
		exec := newSession(ctx, t, is)
		createDDLResumeTable(exec)

		iterator, teardown := ddlResumeIterator(ctx, t, is, nil)
		exec("INSERT INTO " + ddlResumeTable + " VALUES (1, 10, 11)")
		ids, pos := readIDs(ctx, is, iterator, 1)
		is.Equal(ids, []int64{1})
		teardown()

		exec("ALTER TABLE " + ddlResumeTable + " MODIFY COLUMN b VARCHAR(10)")
		exec("INSERT INTO " + ddlResumeTable + " VALUES (2, 20, 'x'), (3, 30, 'y')")

		iterator, teardown = ddlResumeIterator(ctx, t, is, pos)
		defer teardown()
		got, _ := readIDs(ctx, is, iterator, 2)
		if diff := cmp.Diff([]int64{2, 3}, got); diff != "" {
			t.Fatalf("(-want +got):\n%s", diff)
		}
		assertNoMoreIDs(ctx, t, iterator)
	})
}

// Invariant 3: the upgrade path. A position written by a released version has
// no replay_from, so the restart reads the binlog file again from offset 4, and
// an event before the type change lies before the saved event. It is dropped,
// not checked against the changed table.
func TestCDCIterator_UpgradeAfterEarlierTypeChange(t *testing.T) {
	forEachRowMetadata(t, func(t *testing.T, ctx context.Context, is *is.I) {
		exec := newSession(ctx, t, is)
		createDDLResumeTable(exec)

		iterator, teardown := ddlResumeIterator(ctx, t, is, nil)
		exec("INSERT INTO " + ddlResumeTable + " VALUES (1, 10, 11)")
		ids, _ := readIDs(ctx, is, iterator, 1)
		is.Equal(ids, []int64{1})
		exec("ALTER TABLE " + ddlResumeTable + " MODIFY COLUMN b VARCHAR(10)")
		exec("INSERT INTO " + ddlResumeTable + " VALUES (2, 20, 'x'), (3, 30, 'y')")
		ids, pos := readIDs(ctx, is, iterator, 1)
		is.Equal(ids, []int64{2})
		teardown()

		saved, err := common.ParseSDKPosition(pos)
		is.NoErr(err)
		old := *saved.CdcPosition
		old.ReplayFrom, old.Compressed = nil, nil // as a released version wrote it

		iterator, teardown = ddlResumeIterator(ctx, t, is, old.ToSDKPosition())
		defer teardown()
		got, _ := readIDs(ctx, is, iterator, 1)
		if diff := cmp.Diff([]int64{3}, got); diff != "" {
			t.Fatalf("(-want +got):\n%s", diff)
		}
		assertNoMoreIDs(ctx, t, iterator)
	})
}
