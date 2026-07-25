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

// This file is a pure unit test (no database, no *canal.Canal) for cdcRunGate,
// the state machine that fixes the cold-start Teardown/Ack bug found in
// adversarial review of docs/design-documents/20260724-snapshot-cdc-position-handoff.md
// (issue #180):
//
//   - Facet 1 (hang): cdcIterator.Teardown used to wait unconditionally on
//     canalRunDoneC. On a cold start, if the synthetic checkpoint record is
//     never acked (destination down, pipeline stopped early), runFrom is never
//     launched, so nothing would ever close canalRunDoneC - Teardown would hang.
//   - Facet 2 (data race): a late checkpoint Ack racing a concurrent Teardown
//     could call canal.SetEventHandler (an unsynchronized field write in the
//     vendored go-mysql-org/go-mysql library) concurrently with canal.Close
//     (which reads that same field under its own lock) - a genuine data race.
//
// cdcRunGate is deliberately canal-independent (setup/close are passed in as
// closures) specifically so this concurrency behavior can be exercised here,
// under `go test -race`, without requiring a live MySQL connection - which
// constructing a real *canal.Canal does require (canal.NewCanal calls
// checkBinlogRowFormat, which queries the server). A DB-backed
// end-to-end version of this scenario lives in crash_integration_test.go.
package mysql

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCdcRunGate_TeardownNeverBlocksWithoutALaunch is the regression test for
// facet 1: if nothing was ever launched, teardown must return immediately -
// never wait for anything - and any subsequent launch attempt must be refused.
func TestCdcRunGate_TeardownNeverBlocksWithoutALaunch(t *testing.T) {
	var g cdcRunGate

	done := make(chan bool, 1)
	go func() {
		done <- g.teardown(func() {})
	}()

	select {
	case launched := <-done:
		if launched {
			t.Fatal("expected teardown to report no launch was committed")
		}
	case <-time.After(2 * time.Second):
		// Pre-fix equivalent: cdcIterator.Teardown would block forever here on
		// canalRunDoneC, since nothing was ever launched to close it.
		t.Fatal("teardown blocked; it must never wait when nothing was launched")
	}

	setupCalled := false
	if ok := g.tryLaunch(func() { setupCalled = true }); ok {
		t.Fatal("tryLaunch succeeded after teardown")
	}
	if setupCalled {
		t.Fatal("setup was invoked after teardown; a launch must never be committed to once torn down")
	}
}

// TestCdcRunGate_TeardownReportsLaunched covers the mainline (non-cold-start)
// shape: once a launch is committed, teardown must report launched=true so the
// caller (cdcIterator.Teardown) knows it must wait for replication to actually
// stop.
func TestCdcRunGate_TeardownReportsLaunched(t *testing.T) {
	var g cdcRunGate

	if ok := g.tryLaunch(func() {}); !ok {
		t.Fatal("expected tryLaunch to succeed before any teardown")
	}

	if launched := g.teardown(func() {}); !launched {
		t.Fatal("expected teardown to report launched=true")
	}
}

// TestCdcRunGate_LaunchUnconditionally covers the start() (non-cold-start)
// path: it always succeeds and is reported as launched by a later teardown,
// regardless of tornDown (which launchUnconditionally does not even check).
func TestCdcRunGate_LaunchUnconditionally(t *testing.T) {
	var g cdcRunGate

	setupCalled := false
	if ok := g.launchUnconditionally(func() { setupCalled = true }); !ok {
		t.Fatal("launchUnconditionally must always report true")
	}
	if !setupCalled {
		t.Fatal("launchUnconditionally must always call setup")
	}

	if launched := g.teardown(func() {}); !launched {
		t.Fatal("expected teardown to report launched=true after launchUnconditionally")
	}
}

// TestCdcRunGate_ConcurrentTryLaunchAndTeardown_NoDataRace is the regression
// test for facet 2: a late checkpoint ack (tryLaunch) racing a concurrent
// graceful Teardown must never call setup (canal.SetEventHandler in
// production) concurrently with closeFn (canal.Close). Both callbacks touch
// the SAME unsynchronized shared state on purpose - if cdcRunGate's mutex did
// not correctly serialize them, `go test -race` would flag it here. A manually
// checked "busy" flag additionally proves mutual exclusion in wall-clock time,
// independent of whatever the race detector does or doesn't happen to sample.
//
// Run with -race; run repeatedly (the loop below plus `go test -race -count=N`)
// to make the race window realistic to hit if the fix were reverted.
func TestCdcRunGate_ConcurrentTryLaunchAndTeardown_NoDataRace(t *testing.T) {
	for i := 0; i < 200; i++ {
		var g cdcRunGate
		var shared int
		var busy int32
		var overlapped bool

		touch := func() {
			if !atomic.CompareAndSwapInt32(&busy, 0, 1) {
				overlapped = true
			}
			shared++ // deliberately unsynchronized: -race must not flag this if
			// (and only if) the gate correctly serializes every caller of touch.
			time.Sleep(time.Microsecond) // widen the window a real race would need
			atomic.StoreInt32(&busy, 0)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			g.tryLaunch(touch)
		}()
		go func() {
			defer wg.Done()
			g.teardown(touch)
		}()
		wg.Wait()

		if overlapped {
			t.Fatal("setup and close ran concurrently - cdcRunGate failed to serialize them")
		}
		// teardown always calls its closeFn exactly once; tryLaunch calls setup
		// only if it won the race against teardown (i.e. ran first). So touch
		// runs either once (teardown won) or twice (tryLaunch won, then
		// teardown also ran its own closeFn).
		if shared != 1 && shared != 2 {
			t.Fatalf("unexpected shared value %d after one tryLaunch + one teardown", shared)
		}
	}
}
