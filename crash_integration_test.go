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

// This file contains the regression tests for
// docs/design-documents/20260724-snapshot-cdc-position-handoff.md (issue #180):
// the MySQL source connector silently lost writes to already-copied rows across
// a mid-snapshot restart, because the CDC start position (P0) was captured only
// in memory and never persisted. Every existing integration test tore down
// gracefully (iterator.Teardown), which preserves in-memory state and is exactly
// why the bug went unnoticed. These tests instead simulate a hard process kill:
// they abandon the source without calling Teardown (see killSource below), so
// in-memory-only state - including a pre-fix P0 - is genuinely lost, the way it
// would be on a real SIGKILL. A full child-process runner would be more
// faithful still; this in-process abandonment is the "cleanly achievable"
// alternative the design doc calls out.

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// crashRow is a snapshot table with an explicit, non-auto-increment primary
// key, as the design doc requires: a low-key INSERT (e.g. id=2 landing among
// rows 1..N that were already snapshotted) is only reproducible if nothing
// relies on the database's own auto-increment counter, which can only move
// forward.
type crashRow struct {
	ID  int64  `gorm:"primaryKey;autoIncrement:false"`
	Val string `gorm:"size:100"`
}

// killSource simulates a SIGKILL. It deliberately does NOT call
// Source.Teardown/iterator.Teardown: that graceful shutdown path drains acks and
// coordinates a clean stop, which is precisely the behavior that let the pre-fix
// bug hide in every pre-existing integration test (the in-memory-only P0
// survived a graceful teardown). Skipping it means any state that only ever
// lived in memory - like a pre-fix P0 - is genuinely lost, the way it would be
// on a real process kill.
//
// It still force-closes the low-level canal and database connections (bypassing
// the graceful coordination) purely so this test binary does not leak
// goroutines/connections across the many crash scenarios in this file; that
// cleanup has no bearing on what the test is actually verifying.
func killSource(cancel context.CancelFunc, source *Source) {
	cancel()
	forceCloseConnections(source)
}

// forceCloseConnections releases a source's low-level MySQL resources — the
// canal binlog-dump connection and the database pool — WITHOUT the graceful
// Source.Teardown path. This is what killSource uses to model a hard kill, and
// what teardownForCleanup uses for t.Cleanup.
//
// Cleanup must not go through graceful Teardown: a test that stops reading
// before the snapshot fully drains can leave a fetch worker blocked on the
// unbuffered data channel, and snapshotIterator.Teardown's tomb.Wait() (not
// context-bounded) would then block forever — hanging the whole suite until the
// 10-minute go-test timeout. Directly closing the canal here severs the
// binlog-dump connection (which is the resource that, if leaked, starves later
// tests — e.g. TestDestination_OperationCreate timing out), and canal.Close
// unblocks RunFrom so its goroutine exits. Graceful Teardown behaviour is
// verified by each crash test's own assertions, not by cleanup.
func forceCloseConnections(source *Source) {
	if ci, ok := source.iterator.(*combinedIterator); ok {
		if cdc, ok := ci.cdcIterator.(*cdcIterator); ok && cdc.canal != nil {
			cdc.canal.Close()
		}
	}
	if source.db != nil {
		_ = source.db.Close()
	}
}

// teardownForCleanup releases source's connections from a t.Cleanup. It force-
// closes rather than calling graceful Teardown — see forceCloseConnections.
func teardownForCleanup(source *Source) {
	forceCloseConnections(source)
}

// drainRecords reads and acks whatever the source produces until no new record
// has arrived for `quiet`, then returns everything collected. It is used
// instead of a fixed-count read because after a restart the exact mix of
// (possibly duplicate) resumed-snapshot records and replayed CDC records is not
// pinned down by these tests - only the presence of specific records is.
// drainRecordsQuietPeriod is how long drainRecords waits for a new record
// before concluding the stream is (for now) exhausted.
const drainRecordsQuietPeriod = 3 * time.Second

func drainRecords(ctx context.Context, is *is.I, source *Source) []opencdc.Record {
	is.Helper()

	var all []opencdc.Record
	for {
		readCtx, cancel := context.WithTimeout(ctx, drainRecordsQuietPeriod)
		recs, err := source.ReadN(readCtx, 10)
		cancel()

		for _, rec := range recs {
			is.NoErr(source.Ack(ctx, rec.Position))
			all = append(all, rec)
		}

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return all
			}
			is.NoErr(err)
		}
		if len(recs) == 0 {
			return all
		}
	}
}

func parseTestPosition(is *is.I, pos opencdc.Position) common.Position {
	is.Helper()
	parsed, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	return parsed
}

func crashRowKey(rec opencdc.Record) (int64, bool) {
	key, ok := rec.Key.(opencdc.StructuredData)
	if !ok {
		return 0, false
	}
	id, ok := key["id"].(int64)
	return id, ok
}

func hasRecordFor(recs []opencdc.Record, op opencdc.Operation, id int64) bool {
	for _, rec := range recs {
		gotID, ok := crashRowKey(rec)
		if ok && gotID == id && rec.Operation == op {
			return true
		}
	}
	return false
}

// TestCrash_MidSnapshot_SIGKILL_ConcurrentWrites is the primary regression test
// for issue #180. Pre-fix, it fails: the UPDATE/DELETE/low-key-INSERT issued
// against already-copied rows land in the binlog window (P0, P1] that neither
// the resumed snapshot (which only reads rows past LastRead) nor CDC (which,
// pre-fix, starts at a fresh P1 > P0) ever delivers. Post-fix, CDC resumes from
// the persisted P0 and replays them.
func TestCrash_MidSnapshot_SIGKILL_ConcurrentWrites(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})

	// Odd IDs only, leaving even IDs as gaps a low-key INSERT can later land in.
	var rows []crashRow
	for i := 1; i <= 19; i += 2 {
		rows = append(rows, crashRow{ID: int64(i), Val: "initial"})
	}
	is.NoErr(db.Create(&rows).Error)

	cfg := map[string]string{
		"tables":             tableName,
		"snapshot.fetchSize": "5",
	}

	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, nil)

	// Read and ack exactly one chunk (5 records = ids 1,3,5,7,9), so a snapshot
	// position with an advanced LastRead - and, post-fix, cdc_start - is
	// persisted.
	var breakPosition opencdc.Position
	for i := 0; i < 5; i++ {
		recs, err := source.ReadN(ctx, 1)
		is.NoErr(err)
		is.True(len(recs) == 1)
		is.NoErr(source.Ack(ctx, recs[0].Position))
		breakPosition = recs[0].Position
	}

	parsedBreak := parseTestPosition(is, breakPosition)
	is.True(parsedBreak.SnapshotPosition != nil)
	is.True(parsedBreak.SnapshotPosition.CDCStart != nil) // the fix: P0 rides on the first chunk already

	// While "paused" mid-snapshot: mutate already-copied rows (id <= 9).
	is.NoErr(db.Model(&crashRow{}).Where("id = ?", 3).Update("val", "updated").Error)
	is.NoErr(db.Delete(&crashRow{}, 5).Error)
	is.NoErr(db.Create(&crashRow{ID: 2, Val: "low-key-insert"}).Error) // even id, previously unused, <= LastRead(9)

	killSource(cancel, source)

	// Restart from the persisted position.
	restartCtx, restartCancel := context.WithCancel(ctx)
	defer restartCancel()
	source2 := testSourceOpen(restartCtx, is, cfg, breakPosition)
	// source2's canal actually starts replicating (unlike the killed sources
	// above, whose canal killSource explicitly closes) - it must be torn down,
	// or its binlog-dump connection leaks and starves later tests/packages.
	t.Cleanup(func() { teardownForCleanup(source2) })

	all := drainRecords(restartCtx, is, source2)

	is.True(hasRecordFor(all, opencdc.OperationUpdate, 3))
	is.True(hasRecordFor(all, opencdc.OperationDelete, 5))
	is.True(hasRecordFor(all, opencdc.OperationCreate, 2))
}

// TestCrash_DoubleCrash_GuardsClone guards SnapshotPosition.Clone(): if Clone
// dropped CDCStart, records emitted during the *resumed* snapshot would carry
// no cdc_start, and a second mid-snapshot crash would reintroduce the original
// bug. TestCrash_MidSnapshot_SIGKILL_ConcurrentWrites does not catch this, since
// it only crashes once.
func TestCrash_DoubleCrash_GuardsClone(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})

	var rows []crashRow
	for i := 1; i <= 39; i += 2 {
		rows = append(rows, crashRow{ID: int64(i), Val: "initial"})
	}
	is.NoErr(db.Create(&rows).Error)

	cfg := map[string]string{
		"tables":             tableName,
		"snapshot.fetchSize": "5",
	}

	// First run: read+ack one chunk, crash.
	runCtx1, cancel1 := context.WithCancel(ctx)
	source1 := testSourceOpen(runCtx1, is, cfg, nil)

	var firstBreak opencdc.Position
	for i := 0; i < 5; i++ {
		recs, err := source1.ReadN(ctx, 1)
		is.NoErr(err)
		is.True(len(recs) == 1)
		is.NoErr(source1.Ack(ctx, recs[0].Position))
		firstBreak = recs[0].Position
	}
	killSource(cancel1, source1)

	// Second run: resume from firstBreak, read+ack a second chunk, crash again.
	// This exercises the *resumed* snapshot's buildRecord path, which depends
	// on Clone() having preserved CDCStart.
	runCtx2, cancel2 := context.WithCancel(ctx)
	source2 := testSourceOpen(runCtx2, is, cfg, firstBreak)

	var secondBreak opencdc.Position
	for i := 0; i < 5; i++ {
		recs, err := source2.ReadN(ctx, 1)
		is.NoErr(err)
		is.True(len(recs) == 1)
		is.NoErr(source2.Ack(ctx, recs[0].Position))
		secondBreak = recs[0].Position
	}

	parsedSecondBreak := parseTestPosition(is, secondBreak)
	is.True(parsedSecondBreak.SnapshotPosition != nil)
	is.True(parsedSecondBreak.SnapshotPosition.CDCStart != nil) // Clone() must have preserved it

	// Mutate rows already copied in the *second* window (ids 11..19, the
	// second chunk) while paused.
	is.NoErr(db.Model(&crashRow{}).Where("id = ?", 13).Update("val", "updated-again").Error)
	is.NoErr(db.Delete(&crashRow{}, 15).Error)
	is.NoErr(db.Create(&crashRow{ID: 12, Val: "low-key-insert-2"}).Error)

	killSource(cancel2, source2)

	// Third run: resume from secondBreak, read to completion.
	runCtx3, cancel3 := context.WithCancel(ctx)
	defer cancel3()
	source3 := testSourceOpen(runCtx3, is, cfg, secondBreak)
	// source3's canal actually starts replicating and is never killed; it must
	// be torn down or its binlog-dump connection leaks.
	t.Cleanup(func() { teardownForCleanup(source3) })

	all := drainRecords(runCtx3, is, source3)

	is.True(hasRecordFor(all, opencdc.OperationUpdate, 13))
	is.True(hasRecordFor(all, opencdc.OperationDelete, 15))
	is.True(hasRecordFor(all, opencdc.OperationCreate, 12))
}

// TestCrash_AfterSnapshotCompletes_BeforeFirstCDCAck covers the handoff crash:
// the snapshot has fully completed and persisted cdc_start = P0, but the
// connector crashes before the first CDC record is ever acked. Pre-fix this
// case also loses (P0, restart] (a fresh P1 is captured on restart); the fix
// closes it because CDCStart survived on the final snapshot record.
func TestCrash_AfterSnapshotCompletes_BeforeFirstCDCAck(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})

	is.NoErr(db.Create(&crashRow{ID: 1, Val: "initial"}).Error)

	cfg := map[string]string{"tables": tableName}

	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, nil)

	recs, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1)
	is.NoErr(source.Ack(ctx, recs[0].Position))
	snapshotDonePosition := recs[0].Position

	parsed := parseTestPosition(is, snapshotDonePosition)
	is.True(parsed.SnapshotPosition != nil)
	is.True(parsed.SnapshotPosition.CDCStart != nil)

	// Snapshot is done (no more rows); crash before any CDC record is read/acked.
	is.NoErr(db.Model(&crashRow{}).Where("id = ?", 1).Update("val", "updated").Error)
	is.NoErr(db.Create(&crashRow{ID: 2, Val: "created-before-first-cdc-ack"}).Error)

	killSource(cancel, source)

	restartCtx, restartCancel := context.WithCancel(ctx)
	defer restartCancel()
	source2 := testSourceOpen(restartCtx, is, cfg, snapshotDonePosition)
	// source2's canal actually starts replicating and is never killed; it must
	// be torn down or its binlog-dump connection leaks.
	t.Cleanup(func() { teardownForCleanup(source2) })

	all := drainRecords(restartCtx, is, source2)

	is.True(hasRecordFor(all, opencdc.OperationUpdate, 1))
	is.True(hasRecordFor(all, opencdc.OperationCreate, 2))
}
