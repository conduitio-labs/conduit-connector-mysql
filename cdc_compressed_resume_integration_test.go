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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

// Rows inside a compressed transaction (binlog_transaction_compression=ON)
// have no binlog offset of their own: canal reports 0 for each of them and
// syncs offset 0 at their BEGIN and commit. These tests turn compression on
// per session, so they run against any MySQL 8.0.20+ server.

// sessionExec runs statements on one connection, so session variables such as
// binlog_transaction_compression apply to the statements after them.
type sessionExec func(q string)

func newSession(ctx context.Context, t *testing.T, is *is.I) sessionExec {
	t.Helper()
	db := testutils.NewDB(t)
	conn, err := db.SqlxDB.Conn(ctx)
	is.NoErr(err)
	t.Cleanup(func() { _ = conn.Close() })
	return func(q string) {
		_, err := conn.ExecContext(ctx, q)
		is.NoErr(err)
	}
}

func (exec sessionExec) compression(on bool) {
	if on {
		exec("SET SESSION binlog_transaction_compression = ON")
	} else {
		exec("SET SESSION binlog_transaction_compression = OFF")
	}
}

// insertUsers inserts ids from..to in one statement, so one transaction.
func (exec sessionExec) insertUsers(from, to int) {
	var vals []string
	for id := from; id <= to; id++ {
		vals = append(vals, fmt.Sprintf("(%d, 'user-%d', 'user-%d@example.com', NOW())", id, id, id))
	}
	exec("INSERT INTO users (id, username, email, created_at) VALUES " + strings.Join(vals, ","))
}

// Regression test (#228 review, symptom b): a restart from a position inside a
// compressed transaction resumed from offset 0, which re-read the binlog file
// from its start and delivered every row in it again. It must resume exactly
// after the saved row, also when every transaction in the file is compressed.
func TestCDCIterator_ResumeInsideCompressedTransaction(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stopAt   int      // rows read before the restart
		txns     [][2]int // id ranges, one transaction each
		compress []bool
	}{
		{"inside a compressed event", 2, [][2]int{{1, 5}}, []bool{true}},
		{"inside the third of three compressed transactions", 13, [][2]int{{1, 5}, {6, 10}, {11, 15}}, []bool{true, true, true}},
		{"at the end of a compressed transaction", 5, [][2]int{{1, 5}, {6, 8}}, []bool{true, true}},
		{"uncompressed rows after a compressed transaction", 3, [][2]int{{1, 2}, {3, 3}, {4, 5}}, []bool{true, false, false}},
		{"compressed rows after an uncompressed transaction", 4, [][2]int{{1, 3}, {4, 6}}, []bool{false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			db := testutils.NewDB(t)
			testutils.CreateUserTable(is, db)
			exec := newSession(ctx, t, is)

			iterator, teardown := testCdcIterator(ctx, t, is)
			total := 0
			for i, r := range tc.txns {
				exec.compression(tc.compress[i])
				exec.insertUsers(r[0], r[1])
				total += r[1] - r[0] + 1
			}
			ids, pos := readIDs(ctx, is, iterator, tc.stopAt)
			is.Equal(ids, idRange(1, int64(tc.stopAt)))
			teardown()

			saved, err := common.ParseSDKPosition(pos)
			is.NoErr(err)
			is.True(saved.CdcPosition.ReplayFrom != nil)
			is.True(saved.CdcPosition.ReplayFrom.Pos >= binlogFirstEventPos) // a place canal can start from

			iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, pos)
			defer teardown()
			got, _ := readIDs(ctx, is, iterator, total) // more than are left: nothing else may come
			if diff := cmp.Diff(idRange(int64(tc.stopAt+1), int64(total)), got); diff != "" {
				t.Fatalf("resume delivered the wrong rows (-want +got):\n%s", diff)
			}
		})
	}
}

// A compressed statement large enough to be logged as several rows events,
// with two restarts inside it. The events share offset 0 and are told apart
// by their number after the transaction's start.
func TestCDCIterator_ResumeTwiceInsideOneCompressedStatement(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	const n = 3000
	iterator, teardown := testCdcIterator(ctx, t, is)
	exec.compression(true)
	exec.insertUsers(1, n)
	ids, pos := readIDs(ctx, is, iterator, 1200)
	is.Equal(ids, idRange(1, 1200))
	teardown()
	first, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	is.True(first.CdcPosition.Compressed != nil)

	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, pos)
	ids, pos = readIDs(ctx, is, iterator, 1300)
	is.Equal(ids, idRange(1201, 2500))
	teardown()
	second, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	is.True(second.CdcPosition.Compressed.Ordinal > first.CdcPosition.Compressed.Ordinal) // a later rows event

	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, pos)
	defer teardown()
	ids, _ = readIDs(ctx, is, iterator, n)
	if diff := cmp.Diff(idRange(2501, n), ids); diff != "" {
		t.Fatalf("second resume delivered the wrong rows (-want +got):\n%s", diff)
	}
}

// When the captured tables change, the number that names a compressed event
// is no longer comparable: the transaction is delivered again from its
// start. Duplicates, never a gap.
func TestCDCIterator_ResumeInsideCompressedTransactionAfterTablesChanged(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	iterator, teardown := testCdcIterator(ctx, t, is)
	exec.compression(true)
	exec.insertUsers(1, 5)
	exec.insertUsers(6, 10)
	_, pos := readIDs(ctx, is, iterator, 8)
	teardown()

	saved, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	is.True(saved.CdcPosition.Compressed != nil)
	saved.CdcPosition.Compressed.Tables = tablesFingerprint([]string{"users", "another_table"})

	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, saved.CdcPosition.ToSDKPosition())
	defer teardown()
	got, _ := readIDs(ctx, is, iterator, 10)
	if diff := cmp.Diff(idRange(6, 10), got); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

// Regression test (#228 review, symptom a): a position written by an older
// version (no replay_from) re-reads its binlog file from the start. Rows of a
// compressed transaction earlier in the file have offset 0 and were never
// dropped, so they were decoded again, against the table's current columns:
// after an ADD COLUMN that panicked (index out of range) on every restart.
// They lie before the saved event and must be dropped.
func TestCDCIterator_UpgradeWithCompressedTransactionAndLaterDDL(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	iterator, teardown := testCdcIterator(ctx, t, is)
	exec.compression(true)
	exec.insertUsers(1, 2)
	exec.compression(false)
	exec.insertUsers(10, 10)
	ids, _ := readIDs(ctx, is, iterator, 3)
	is.Equal(ids, []int64{1, 2, 10})
	exec("ALTER TABLE users ADD COLUMN extra INT NULL")
	exec("INSERT INTO users (id, username, email, created_at, extra) VALUES " +
		"(3, 'user-3', 'user-3@example.com', NOW(), 7), (4, 'user-4', 'user-4@example.com', NOW(), 8)")
	ids, pos := readIDs(ctx, is, iterator, 1)
	is.Equal(ids, []int64{3})
	teardown()

	saved, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	old := *saved.CdcPosition
	old.ReplayFrom, old.Compressed = nil, nil // as an older version wrote it

	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, old.ToSDKPosition())
	defer teardown()
	got, _ := readIDs(ctx, is, iterator, 10)
	if diff := cmp.Diff([]int64{4}, got); diff != "" {
		t.Fatalf("(-want +got):\n%s", diff)
	}
}

// Invariant 6: a binlog event written before a column was added, read after
// it, has fewer values than the table has columns. The values cannot be
// mapped, so the source halts with a stable code instead of panicking.
func TestCDCIterator_RowWithFewerValuesThanColumnsHalts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	iterator, teardown := testCdcIterator(ctx, t, is)
	exec.insertUsers(1, 1)
	_, pos := readIDs(ctx, is, iterator, 1)
	teardown()

	// Written while the connector is down; read after the ALTER ran.
	exec.insertUsers(2, 2)
	exec("ALTER TABLE users ADD COLUMN extra INT NULL")

	iterator, _ = testCdcIteratorAtPosition(ctx, t, is, pos)
	err := readUntilError(ctx, iterator)
	var coded *Error
	is.True(errors.As(err, &coded))
	is.Equal(coded.Code, CodeRowColumnsMismatch)
	is.True(errors.Is(err, ErrRowColumnsMismatch))
	is.True(errors.Is(iterator.Teardown(ctx), ErrRowColumnsMismatch)) // the halt is reported, not swallowed
}

// A saved index past the end of its event means the position does not
// describe this binlog. It used to be logged and the event's rows skipped;
// the source must halt instead.
func TestCDCIterator_SavedIndexPastItsEventHalts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	iterator, teardown := testCdcIterator(ctx, t, is)
	exec.insertUsers(1, 3)
	exec.insertUsers(4, 4)
	_, pos := readIDs(ctx, is, iterator, 1)
	teardown()

	saved, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	saved.CdcPosition.Index = 10 // the event has 3 rows

	iterator, _ = testCdcIteratorAtPosition(ctx, t, is, saved.CdcPosition.ToSDKPosition())
	err = readUntilError(ctx, iterator)
	is.True(errors.Is(err, ErrResumeEventNotFound))
	var coded *Error
	is.True(errors.As(err, &coded))
	is.Equal(coded.Code, CodeResumeEventNotFound)
	is.True(errors.Is(iterator.Teardown(ctx), ErrResumeEventNotFound))
}

// readUntilError reads and acks until ReadN fails with something other than a
// quiet period, and returns that error. Records are not expected.
func readUntilError(ctx context.Context, it common.Iterator) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		recs, err := it.ReadN(readCtx, 10)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("read: %w", err)
		}
		for _, r := range recs {
			if err := it.Ack(ctx, r.Position); err != nil {
				return fmt.Errorf("ack: %w", err)
			}
			if sd, ok := r.Key.(opencdc.StructuredData); ok {
				return fmt.Errorf("unexpected record %v", sd)
			}
		}
	}
	return errors.New("no error within 10s")
}
