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

// Snapshot->CDC handoff under a WITHHELD ack.
//
// crash_integration_test.go covers the class where the CDC start position is
// lost across a restart (issue #180, "class B"). Every test there — and every
// other integration test in this repo — acks every record it reads. That
// leaves a different class entirely uncovered:
//
//	the snapshot iterator gates its own completion on receiving acks.
//	snapshot_iterator.go's Read waits on s.acks.Wait(ctx) before returning
//	ErrSnapshotIteratorDone, and s.acks.Done() is called ONLY from Ack. So a
//	boundary ack that never arrives means the handoff never completes and CDC
//	never starts.
//
// That is the shape of Conduit's own sev-0 in
// docs/postmortems/20260729-snapshot-handoff-deferred-ack-deadlock.md, where
// the Postgres source blocked forever on exactly this and every post-snapshot
// change was silently lost — no error, no DLQ, pipeline apparently "running".
// That postmortem's follow-up list asks explicitly whether MySQL and MongoDB
// gate a handoff on the ack the same way. MySQL does. These tests establish
// what actually happens when the ack does not come.
//
// The engine is not supposed to drop that ack — Conduit#2707 fixed the path
// that did. These tests are about what the CONNECTOR does if it ever happens
// again, from any cause: a stalled destination, a bug, an operator pause.
// "The engine promises not to" is not a property the connector can rely on.

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/matryer/is"
)

// handoffStallTimeout bounds how long we let a withheld-ack read block before
// calling it stalled. Generous relative to the work involved (one row), so a
// slow CI box cannot make this flaky in the "declares a stall that isn't one"
// direction.
const handoffStallTimeout = 10 * time.Second

// TestHandoff_WithheldBoundaryAck_DoesNotHangForever asserts the connector does
// not block indefinitely when the final snapshot ack never arrives.
//
// Read the assertion carefully: it does NOT require the handoff to succeed. A
// connector that gates on acks is entitled to wait. What it is not entitled to
// do is wait forever with no way out, because that presents to an operator as a
// running pipeline that has silently stopped moving data.
//
// Passing means the read returned — either with records, or with a context
// error once the caller's deadline elapsed. Failing means the call was still
// blocked when the test's own (much longer) deadline hit, i.e. the caller's
// context did not get it out.
func TestHandoff_WithheldBoundaryAck_DoesNotHangForever(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})

	is.NoErr(db.Create(&crashRow{ID: 1, Val: "only-row"}).Error)

	cfg := map[string]string{"tables": tableName}
	source := testSourceOpen(ctx, is, cfg, nil)
	t.Cleanup(func() { teardownForCleanup(source) })

	// Read the one snapshot row and DELIBERATELY DO NOT ACK IT. This is the
	// whole point: acks.Done() is only reached from Ack, so the waitgroup the
	// snapshot's completion path waits on never reaches zero.
	recs, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1)

	// Now ask for the next record. The snapshot has no more rows, so the
	// iterator must decide it is done — which is where it waits on the acks
	// that will never arrive.
	readCtx, cancel := context.WithTimeout(ctx, handoffStallTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, readErr := source.ReadN(readCtx, 1)
		done <- readErr
	}()

	select {
	case readErr := <-done:
		// Returned. Either it got past the handoff, or the caller's context
		// pulled it out. Both are acceptable; a permanent hang is not.
		if readErr != nil && !errors.Is(readErr, context.DeadlineExceeded) &&
			!errors.Is(readErr, context.Canceled) {
			t.Logf("read returned a non-context error (acceptable, recorded): %v", readErr)
		}
	case <-time.After(handoffStallTimeout + 15*time.Second):
		t.Fatal("snapshot->CDC handoff blocked past its own context deadline with the " +
			"boundary ack withheld: the caller's context could not interrupt it. This is the " +
			"shape of the Postgres sev-0 in Conduit's 20260729 postmortem — a pipeline that " +
			"looks running while no post-snapshot change is ever delivered")
	}
}

// TestHandoff_WithheldBoundaryAck_TeardownBoundedWithoutCallerDeadline is the
// regression test for the actual production hang, and it is the one that would
// have caught this.
//
// The two tests around it pass a context WITH a deadline, so they prove only
// that the wait is cancellable — which was already true, and which is why an
// earlier pass at this concluded "cancellable, no deadlock" and moved on. The
// SDK does not pass a deadline: sourcePluginAdapter.Teardown hands the plugin
// an undeadlined context. Reproduced against real MySQL, Teardown never
// returned.
//
// So this test matches what production actually does. Before the bound in
// DefaultTeardownAckTimeout it hung indefinitely; with it, Teardown returns.
func TestHandoff_WithheldBoundaryAck_TeardownBoundedWithoutCallerDeadline(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})

	is.NoErr(db.Create(&crashRow{ID: 1, Val: "only-row"}).Error)

	source := testSourceOpen(ctx, is, map[string]string{"tables": tableName}, nil)

	recs, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1) // read and DELIBERATELY not acked

	done := make(chan error, 1)
	// context.Background(): no deadline, exactly as the SDK calls Teardown.
	go func() { done <- source.Teardown(context.Background()) }()

	select {
	case tdErr := <-done:
		// The bound fired and teardown proceeded. It must not report an error:
		// un-arrived acks are a warning condition, not a teardown failure.
		is.NoErr(tdErr)
	case <-time.After(DefaultTeardownAckTimeout + 20*time.Second):
		t.Fatal("Teardown with an undeadlined context did not return: graceful shutdown " +
			"cannot complete, so an operator must kill -9 a pipeline that reports itself " +
			"as stopping (invariant 7). This is the exact production shape — the SDK " +
			"passes no deadline")
	}
}

// TestHandoff_WithheldBoundaryAck_TeardownDoesNotHang covers the second half,
// and it is the one that decides whether invariant 7 (SIGTERM drains and
// checkpoints before exit) holds.
//
// snapshot_iterator.go's Teardown waits on the SAME waitgroup as the completion
// path. The SDK calls the plugin's Teardown with no timeout of its own, so if
// that wait cannot be interrupted, a graceful shutdown never finishes and the
// process has to be killed — turning a clean stop into a hard one.
func TestHandoff_WithheldBoundaryAck_TeardownDoesNotHang(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &crashRow{})
	tableName := testutils.TableName(is, db, &crashRow{})

	is.NoErr(db.Create(&crashRow{ID: 1, Val: "only-row"}).Error)

	cfg := map[string]string{"tables": tableName}
	source := testSourceOpen(ctx, is, cfg, nil)

	// Read without acking, as above.
	recs, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1)

	tdCtx, cancel := context.WithTimeout(ctx, handoffStallTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- source.Teardown(tdCtx) }()

	select {
	case tdErr := <-done:
		t.Logf("teardown returned with the boundary ack withheld: %v", tdErr)
	case <-time.After(handoffStallTimeout + 15*time.Second):
		t.Fatal("Teardown blocked past its own context deadline with the boundary ack " +
			"withheld. The SDK applies no timeout to the plugin's Teardown, so a graceful " +
			"stop can never complete: invariant 7 (SIGTERM drains before exit) is violated " +
			"and the operator is forced to kill -9 a pipeline that reports itself as stopping")
	}
}
