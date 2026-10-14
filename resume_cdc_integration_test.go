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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func countSnapshots(recs []opencdc.Record) int {
	n := 0
	for _, rec := range recs {
		if rec.Operation == opencdc.OperationSnapshot {
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
	is.Equal(countSnapshots(first), 3)
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

	is.Equal(countSnapshots(second), 0) // the #158 assertion
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
	is.Equal(countSnapshots(got), 6) // none of 1..4 repeated
}

// lateRow is a second table, used to model a table added to the configuration
// after the pipeline entered CDC.
type lateRow struct {
	ID  int64  `gorm:"primaryKey;autoIncrement:false"`
	Val string `gorm:"size:100"`
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p) //nolint:wrapcheck // test helper
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// capturedLogContext returns a context whose logger writes to the returned buffer.
func capturedLogContext() (context.Context, *syncBuffer) {
	buf := &syncBuffer{}
	return zerolog.New(buf).WithContext(context.Background()), buf
}

// openSource is testSourceOpen that returns Open's error instead of failing.
func openSource(ctx context.Context, is *is.I, cfg map[string]string, pos opencdc.Position) (*Source, error) {
	source := &Source{}
	cfg["dsn"] = testutils.DSN
	cfg["cdc.disableLogs"] = "true"
	is.NoErr(sdk.Util.ParseConfig(ctx, cfg, source.Config(), Connector.NewSpecification().SourceParams))
	return source, source.Open(ctx, pos)
}

// cdcPositionAfterOneChange opens a source on cfg from scratch, writes one row
// and returns the position of the last record, once the source is killed.
func cdcPositionAfterOneChange(ctx context.Context, is *is.I, db testutils.DB, cfg map[string]string) opencdc.Position {
	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, nil)
	is.NoErr(db.Create(&crashRow{ID: 100, Val: "x"}).Error)
	recs := drainRecords(ctx, is, source)
	is.True(len(recs) > 0)
	pos := recs[len(recs)-1].Position
	is.True(parseTestPosition(is, pos).CdcPosition != nil)
	killSource(cancel, source)
	return pos
}

func coverageOf(is *is.I, pos opencdc.Position) common.TableCoverage {
	is.Helper()
	cdc := parseTestPosition(is, pos).CdcPosition
	is.True(cdc != nil)
	is.True(cdc.TableCoverage != nil)
	return *cdc.TableCoverage
}

func snapshotted(is *is.I, pos opencdc.Position) []string {
	is.Helper()
	c := coverageOf(is, pos)
	if c.Snapshotted == nil {
		return []string{}
	}
	return c.Snapshotted
}

func resetTables(is *is.I, db testutils.DB) {
	is.NoErr(db.Migrator().DropTable(&lateRow{}))
	testutils.CreateTables(is, db, &crashRow{})
}

// Review blocker on #195: a table added to an explicit `tables` list after CDC
// began used to lose its existing rows silently. Open must refuse.
func TestRestart_TableAddedAfterCDC_ExplicitList_Fails(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)

	testutils.CreateTables(is, db, &lateRow{})
	is.NoErr(db.Create(&lateRow{ID: 1, Val: "never delivered"}).Error)

	pos := cdcPositionAfterOneChange(ctx, is, db, map[string]string{"tables": "crash_rows"})
	is.Equal(snapshotted(is, pos), []string{"crash_rows"})

	_, err := openSource(ctx, is, map[string]string{"tables": "crash_rows,late_rows"}, pos)
	is.True(errors.Is(err, ErrTablesAddedAfterCDC))
	var coded *Error
	is.True(errors.As(err, &coded))
	is.Equal(coded.Code, "mysql.snapshot.tables_added_after_cdc")
	is.Equal(coded.ConfigPath, "tables")
	is.True(strings.Contains(coded.Message, "late_rows"))
	is.True(strings.Contains(coded.Fix, "^name$"))
}

// A table removed from the configuration and added back later is not assumed
// covered: changes made while it was out of `tables` were never captured.
func TestRestart_TableRemovedThenReadded_IsDetected(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	testutils.CreateTables(is, db, &lateRow{})
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)
	is.NoErr(db.Create(&lateRow{ID: 1, Val: "b"}).Error)

	both := map[string]string{"tables": "crash_rows,late_rows"}
	pos1 := cdcPositionAfterOneChange(ctx, is, db, both)
	is.Equal(snapshotted(is, pos1), []string{"crash_rows", "late_rows"})

	// Run 2 without late_rows: the coverage forgets it.
	runCtx, cancel := context.WithCancel(ctx)
	source, err := openSource(runCtx, is, map[string]string{"tables": "crash_rows"}, pos1)
	is.NoErr(err)
	is.NoErr(db.Create(&crashRow{ID: 200, Val: "x"}).Error)
	recs := drainRecords(ctx, is, source)
	is.True(len(recs) > 0)
	pos2 := recs[len(recs)-1].Position
	is.Equal(snapshotted(is, pos2), []string{"crash_rows"})
	killSource(cancel, source)

	// Run 3 adds it back: refused.
	_, err = openSource(ctx, is, map[string]string{"tables": "crash_rows,late_rows"}, pos2)
	is.True(errors.Is(err, ErrTablesAddedAfterCDC))
}

func TestRestart_TableAddedAfterCDC_Pattern_WarnsOnly(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)

	// late_rows already holds data before the pipeline's first run, but is not
	// matched by its first configuration.
	testutils.CreateTables(is, db, &lateRow{})
	is.NoErr(db.Create(&lateRow{ID: 1, Val: "existing"}).Error)

	pos := cdcPositionAfterOneChange(ctx, is, db, map[string]string{"tables": "crash_rows"})
	is.Equal(snapshotted(is, pos), []string{"crash_rows"})

	logCtx, logs := capturedLogContext()
	runCtx, cancel := context.WithCancel(logCtx)
	defer cancel()
	source, err := openSource(runCtx, is, map[string]string{"tables": "^(crash|late)_rows$"}, pos)
	is.NoErr(err)
	t.Cleanup(func() { teardownForCleanup(source) })

	is.True(strings.Contains(logs.String(), `"level":"warn"`))
	is.True(strings.Contains(logs.String(), "mysql.snapshot.tables_added_after_cdc"))
	is.True(strings.Contains(logs.String(), "late_rows"))

	// Documented limitation: existing rows of the new table are not delivered,
	// changes after the restart are.
	is.NoErr(db.Create(&lateRow{ID: 2, Val: "new"}).Error)
	got := drainRecords(runCtx, is, source)
	is.Equal(countSnapshots(got), 0)
	is.Equal(len(got), 1)
	is.Equal(got[0].Operation, opencdc.OperationCreate)
	c := coverageOf(is, got[0].Position)
	is.Equal(c.Snapshotted, []string{"crash_rows"}) // never claimed as snapshotted
	is.Equal(c.CDCOnly, []string{"late_rows"})      // recorded, so the warning is not repeated
	killSource(cancel, source)

	// The next restart is silent about it.
	logCtx2, logs2 := capturedLogContext()
	runCtx2, cancel2 := context.WithCancel(logCtx2)
	defer cancel2()
	source2, err := openSource(runCtx2, is, map[string]string{"tables": "^(crash|late)_rows$"}, got[0].Position)
	is.NoErr(err)
	t.Cleanup(func() { teardownForCleanup(source2) })
	is.True(!strings.Contains(logs2.String(), `"level":"warn"`))
}

// Same rule when snapshot.enabled is switched on for a pipeline that has been
// running CDC-only.
func TestRestart_SnapshotFlippedOn_AfterCDCOnly_Fails(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "existing"}).Error)

	pos := cdcPositionAfterOneChange(ctx, is, db, map[string]string{"tables": "crash_rows", "snapshot.enabled": "false"})
	is.Equal(snapshotted(is, pos), []string{}) // known: nothing snapshotted

	_, err := openSource(ctx, is, map[string]string{"tables": "crash_rows"}, pos)
	is.True(errors.Is(err, ErrTablesAddedAfterCDC))

	// Staying CDC-only is fine.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	source, err := openSource(runCtx, is, map[string]string{"tables": "crash_rows", "snapshot.enabled": "false"}, pos)
	is.NoErr(err)
	t.Cleanup(func() { teardownForCleanup(source) })
}

// Upgrade test: a cdc_position written by the previous build (no table_coverage)
// opened by this build must resume and adopt the configured tables as
// UNVERIFIED, never as snapshotted, warning on every start; tables added
// afterwards are detected.
func TestUpgrade_LegacyCDCPosition_AdoptsTablesAsUnverified(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	testutils.CreateTables(is, db, &lateRow{})
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)

	cfg := func() map[string]string { return map[string]string{"tables": "crash_rows"} }
	pos := cdcPositionAfterOneChange(ctx, is, db, cfg())

	// Rewrite it exactly as the previous build would have written it.
	var raw map[string]any
	is.NoErr(json.Unmarshal(pos, &raw))
	delete(raw["cdc_position"].(map[string]any), "table_coverage")
	legacy, err := json.Marshal(raw)
	is.NoErr(err)
	is.True(!strings.Contains(string(legacy), "table_coverage"))

	is.NoErr(db.Create(&crashRow{ID: 101, Val: "after-upgrade"}).Error)

	logCtx, logs := capturedLogContext()
	runCtx, cancel := context.WithCancel(logCtx)
	source, err := openSource(runCtx, is, cfg(), legacy)
	is.NoErr(err)
	is.True(strings.Contains(logs.String(), `"level":"warn"`))
	is.True(strings.Contains(logs.String(), "mysql.snapshot.tables_unverified"))
	is.True(strings.Contains(logs.String(), "ASSUMED"))

	got := drainRecords(runCtx, is, source)
	is.Equal(countSnapshots(got), 0)
	is.True(hasRecordFor(got, opencdc.OperationCreate, 101))
	upgraded := got[len(got)-1].Position
	c := coverageOf(is, upgraded)
	is.Equal(c.Unverified, []string{"crash_rows"})
	is.Equal(len(c.Snapshotted), 0) // never claimed covered
	killSource(cancel, source)

	// Restart: still warned, still unverified.
	logCtx2, logs2 := capturedLogContext()
	runCtx2, cancel2 := context.WithCancel(logCtx2)
	source2, err := openSource(runCtx2, is, cfg(), upgraded)
	is.NoErr(err)
	is.True(strings.Contains(logs2.String(), "mysql.snapshot.tables_unverified"))
	killSource(cancel2, source2)

	// A table added afterwards is detected, relative to the adopted set.
	_, err = openSource(ctx, is, map[string]string{"tables": "crash_rows,late_rows"}, upgraded)
	is.True(errors.Is(err, ErrTablesAddedAfterCDC))
}

// Crash between the last snapshot ack and the first CDC ack, through Open:
// nothing is re-snapshotted, nothing is lost, and the unacked CDC record is
// delivered again. Invariants 1-3.
func TestCrash_BetweenLastSnapshotAckAndFirstCDCAck_ThroughOpen(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	is.NoErr(db.Create(&[]crashRow{{ID: 1, Val: "a"}, {ID: 2, Val: "b"}, {ID: 3, Val: "c"}}).Error)

	cfg := map[string]string{"tables": "crash_rows", "snapshot.fetchSize": "2"}

	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, nil)

	var lastSnapshotPos opencdc.Position
	for range 3 {
		recs, err := source.ReadN(ctx, 1)
		is.NoErr(err)
		is.Equal(len(recs), 1)
		is.Equal(recs[0].Operation, opencdc.OperationSnapshot)
		is.NoErr(source.Ack(ctx, recs[0].Position))
		lastSnapshotPos = recs[0].Position
	}
	is.True(parseTestPosition(is, lastSnapshotPos).SnapshotPosition != nil)

	// A change after the snapshot: read it (first CDC record) but never ack it.
	is.NoErr(db.Create(&crashRow{ID: 4, Val: "d"}).Error)
	recs, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	is.Equal(recs[0].Operation, opencdc.OperationCreate)
	killSource(cancel, source)

	// Changes while down.
	is.NoErr(db.Create(&crashRow{ID: 5, Val: "e"}).Error)

	restartCtx, restartCancel := context.WithCancel(ctx)
	defer restartCancel()
	source2 := testSourceOpen(restartCtx, is, cfg, lastSnapshotPos)
	t.Cleanup(func() { teardownForCleanup(source2) })

	got := drainRecords(restartCtx, is, source2)
	// The resumed snapshot recomputes its end from the table as it is now, so
	// rows inserted since (4, 5) may also arrive as snapshot records: duplicates
	// of what CDC replays, never a gap. Rows 1-3 must not be read again.
	for _, id := range []int64{1, 2, 3} {
		is.True(!hasRecordFor(got, opencdc.OperationSnapshot, id))
	}
	is.True(hasRecordFor(got, opencdc.OperationCreate, 4)) // unacked record redelivered
	is.True(hasRecordFor(got, opencdc.OperationCreate, 5)) // downtime change delivered
	is.Equal(snapshotted(is, got[len(got)-1].Position), []string{"crash_rows"})
}
