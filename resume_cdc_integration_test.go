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

// Regression tests for issue #158: restarting from a persisted cdc_position
// re-ran the snapshot of every table before CDC resumed, so every restart in
// steady state re-delivered the whole dataset and delayed CDC behind a full
// table scan. Invariant 3: a restart must resume from the persisted position,
// not from scratch.

package mysql

import (
	"context"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func countOps(recs []opencdc.Record, op opencdc.Operation) int {
	n := 0
	for _, rec := range recs {
		if rec.Operation == op {
			n++
		}
	}
	return n
}

// TestRestart_FromCDCPosition_SkipsSnapshot fails on main: the restart emits
// the three pre-existing rows again as snapshot records.
func TestRestart_FromCDCPosition_SkipsSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})

	is.NoErr(db.Create(&[]crashRow{
		{ID: 1, Val: "a"}, {ID: 2, Val: "b"}, {ID: 3, Val: "c"},
	}).Error)

	cfg := map[string]string{"tables": tableName}

	// Run 1: snapshot three rows, then one CDC record.
	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, nil)
	is.NoErr(db.Create(&crashRow{ID: 4, Val: "d"}).Error)

	first := drainRecords(ctx, is, source)
	is.Equal(countOps(first, opencdc.OperationSnapshot), 3)
	is.True(hasRecordFor(first, opencdc.OperationCreate, 4))

	lastPos := first[len(first)-1].Position
	is.True(parseTestPosition(is, lastPos).CdcPosition != nil) // steady state

	killSource(cancel, source)

	// Changes made while the connector is down must still be delivered.
	is.NoErr(db.Create(&crashRow{ID: 5, Val: "e"}).Error)
	is.NoErr(db.Model(&crashRow{}).Where("id = ?", 1).Update("val", "a2").Error)

	// Run 2: restart from the CDC position.
	restartCtx, restartCancel := context.WithCancel(ctx)
	defer restartCancel()
	source2 := testSourceOpen(restartCtx, is, cfg, lastPos)
	t.Cleanup(func() { teardownForCleanup(source2) })

	second := drainRecords(restartCtx, is, source2)

	is.Equal(countOps(second, opencdc.OperationSnapshot), 0) // the #158 assertion
	is.True(hasRecordFor(second, opencdc.OperationCreate, 5))
	is.True(hasRecordFor(second, opencdc.OperationUpdate, 1))
	is.Equal(len(second), 2) // nothing re-delivered from before the position
}

// TestRestart_FromCDCPosition_Repeatedly guards the hand-over across several
// restarts: each run must pick up exactly where the previous one stopped.
func TestRestart_FromCDCPosition_Repeatedly(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)

	cfg := map[string]string{"tables": tableName}

	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, nil)
	is.NoErr(db.Create(&crashRow{ID: 2, Val: "b"}).Error)
	recs := drainRecords(ctx, is, source)
	is.Equal(len(recs), 2)
	pos := recs[len(recs)-1].Position
	killSource(cancel, source)

	for id := int64(3); id <= 5; id++ {
		is.NoErr(db.Create(&crashRow{ID: id, Val: "x"}).Error)

		runCtx, cancel := context.WithCancel(ctx)
		src := testSourceOpen(runCtx, is, cfg, pos)
		got := drainRecords(ctx, is, src)

		is.Equal(len(got), 1)
		is.True(hasRecordFor(got, opencdc.OperationCreate, id))
		pos = got[0].Position
		is.True(parseTestPosition(is, pos).CdcPosition != nil)
		killSource(cancel, src)
	}
}

// TestRestart_FromSnapshotPosition_ResumesSnapshot is the guard against
// over-correcting #158: a position taken mid-snapshot must keep resuming the
// snapshot, not jump to CDC and skip the rows not yet read.
func TestRestart_FromSnapshotPosition_ResumesSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})

	var rows []crashRow
	for i := 1; i <= 10; i++ {
		rows = append(rows, crashRow{ID: int64(i), Val: "v"})
	}
	is.NoErr(db.Create(&rows).Error)

	cfg := map[string]string{"tables": tableName, "snapshot.fetchSize": "3"}

	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, nil)

	var pos opencdc.Position
	for range 4 {
		recs, err := source.ReadN(ctx, 1)
		is.NoErr(err)
		is.True(len(recs) == 1)
		is.NoErr(source.Ack(ctx, recs[0].Position))
		pos = recs[0].Position
	}
	is.True(parseTestPosition(is, pos).SnapshotPosition != nil)
	killSource(cancel, source)

	restartCtx, restartCancel := context.WithCancel(ctx)
	defer restartCancel()
	source2 := testSourceOpen(restartCtx, is, cfg, pos)
	t.Cleanup(func() { teardownForCleanup(source2) })

	got := drainRecords(restartCtx, is, source2)

	for id := int64(5); id <= 10; id++ {
		is.True(hasRecordFor(got, opencdc.OperationSnapshot, id)) // no row skipped
	}
	is.Equal(countOps(got, opencdc.OperationSnapshot), 6) // none of 1..4 repeated
}
