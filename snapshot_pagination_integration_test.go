// Copyright © 2024 Meroxa, Inc.
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
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

type compositeRow struct {
	A int    `gorm:"primaryKey;autoIncrement:false"`
	B int    `gorm:"primaryKey;autoIncrement:false"`
	V string `gorm:"size:20"`
}

// compositeRows has rows on both failure shapes of a per-column "col > ?" seek:
// rows with the same a and a larger b, and rows with a larger a and a smaller b,
// relative to every possible chunk boundary.
func compositeRows() []compositeRow {
	var rows []compositeRow
	for a := 1; a <= 4; a++ {
		for b := 1; b <= 3; b++ {
			rows = append(rows, compositeRow{A: a, B: b, V: fmt.Sprintf("%d-%d", a, b)})
		}
	}
	return rows
}

// readSnapshot reads a snapshot of table, acking every record. It stops after
// limit records when limit > 0, otherwise it reads to completion. It returns the
// "v" payload values in delivery order and the last record's position.
func readSnapshot(
	ctx context.Context, t *testing.T, table string, keys common.PrimaryKeys,
	fetchSize uint64, start *common.SnapshotPosition, limit int,
) (vals []string, last opencdc.Position) {
	t.Helper()
	is := is.New(t)

	db := testutils.NewDB(t).SqlxDB
	serverID, err := common.GetServerID(ctx, db)
	is.NoErr(err)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tablePrimaryKeys: common.TableKeys{table: keys},
		db:               db,
		database:         "meroxadb",
		serverID:         serverID,
		fetchSize:        fetchSize,
		startPosition:    start,
	})
	is.NoErr(err)
	is.NoErr(iterator.setupWorkers(ctx))
	iterator.start(ctx)

	for limit == 0 || len(vals) < limit {
		recs, err := iterator.ReadN(ctx, 1)
		if errors.Is(err, ErrSnapshotIteratorDone) {
			break
		}
		is.NoErr(err)
		for _, rec := range recs {
			is.NoErr(iterator.Ack(ctx, rec.Position))
			vals = append(vals, rec.Payload.After.(opencdc.StructuredData)["v"].(string))
			last = rec.Position
		}
	}
	is.NoErr(iterator.Teardown(ctx))

	return vals, last
}

func compositeSetup(t *testing.T) (string, []string) {
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &compositeRow{})
	rows := compositeRows()
	is.NoErr(db.Create(&rows).Error)

	want := make([]string, len(rows))
	for i, r := range rows {
		want[i] = r.V
	}
	return testutils.TableName(is, db, &compositeRow{}), want
}

// Issue #189: a composite-key table larger than snapshot.fetchSize must be
// delivered completely, in key order, exactly once. Invariant 3.
func TestSnapshotIterator_CompositeKey_MultipleChunks(t *testing.T) {
	ctx := testutils.TestContext(t)
	table, want := compositeSetup(t)

	for _, fetchSize := range []uint64{1, 2, 3, 5, 11, 12, 13} {
		t.Run(fmt.Sprintf("fetchSize=%d", fetchSize), func(t *testing.T) {
			is := is.New(t)
			got, _ := readSnapshot(ctx, t, table, common.PrimaryKeys{"a", "b"}, fetchSize, nil, 0)
			is.Equal(got, want)
		})
	}
}

// Resuming from the position of the k-th record, round-tripped through the
// persisted JSON form, must deliver exactly the remaining rows: no gap, no repeat.
func TestSnapshotIterator_CompositeKey_ResumeFromEveryPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	table, want := compositeSetup(t)

	for k := 1; k < len(want); k++ {
		t.Run(fmt.Sprintf("after=%d", k), func(t *testing.T) {
			is := is.New(t)
			head, pos := readSnapshot(ctx, t, table, common.PrimaryKeys{"a", "b"}, 2, nil, k)
			is.Equal(head, want[:k])

			parsed, err := common.ParseSDKPosition(pos)
			is.NoErr(err)
			is.True(parsed.SnapshotPosition != nil)

			tail, _ := readSnapshot(ctx, t, table, common.PrimaryKeys{"a", "b"}, 2, parsed.SnapshotPosition, 0)
			is.Equal(tail, want[k:])
		})
	}
}

// Positions written by builds that predate key names in composite positions
// (key_name is empty) must still resume correctly.
func TestSnapshotIterator_CompositeKey_ResumeFromLegacyPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	table, want := compositeSetup(t)

	legacy := fmt.Sprintf(`{"snapshots":{%q:{"MultipleKey":[`+
		`{"key_name":"","last_read":2,"snapshot_end":null},`+
		`{"key_name":"","last_read":2,"snapshot_end":null}]}}}`, table)
	var pos common.Position
	is.NoErr(json.Unmarshal([]byte(`{"snapshot_position":`+legacy+`}`), &pos))

	tail, _ := readSnapshot(ctx, t, table, common.PrimaryKeys{"a", "b"}, 2, pos.SnapshotPosition, 0)
	is.Equal(tail, want[5:]) // rows after (2,2)
}
