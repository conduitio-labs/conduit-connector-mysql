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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

// oneRowWorker hands the snapshot iterator a single row and finishes, like a
// fetch worker for a one-row table.
type oneRowWorker struct {
	data chan fetchData
}

func (w oneRowWorker) table() string { return "users" }

func (w oneRowWorker) fetchStartEnd(context.Context) (bool, error) { return false, nil }

func (w oneRowWorker) run(ctx context.Context) error {
	select {
	case w.data <- fetchData{
		table:         "users",
		key:           opencdc.StructuredData{"id": int64(1)},
		payload:       opencdc.StructuredData{"id": int64(1)},
		position:      common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: int64(1), SnapshotEnd: int64(1)}},
		payloadSchema: &schemaSubjectVersion{subject: "users_payload", version: 1},
		keySchema:     &schemaSubjectVersion{subject: "users_key", version: 1},
	}:
	case <-ctx.Done():
	}
	return nil
}

// Regression test: waiting for snapshot acks used csync.WaitGroup, whose Wait
// starts a goroutine that blocks until the counter reaches zero. A wait that
// gave up (a ReadN whose context ended during the snapshot-to-CDC handoff, and
// Teardown's bounded wait) left that goroutine behind for as long as the acks
// never came, which after a stop is forever. Found by acceptance suite v2
// (SRC-07, SRC-08 goroutine-leak checks).
func TestSnapshotIterator_AbandonedAckWaitsDoNotLeak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	is := is.New(t)
	ctx := context.Background()

	prevTimeout := DefaultTeardownAckTimeout
	DefaultTeardownAckTimeout = 20 * time.Millisecond
	t.Cleanup(func() { DefaultTeardownAckTimeout = prevTimeout })

	it, err := newSnapshotIterator(snapshotIteratorConfig{
		tablePrimaryKeys: common.TableKeys{"users": {"id"}},
		database:         "meroxadb",
		serverID:         "1",
	})
	is.NoErr(err)
	it.setCDCStart(&common.CdcPosition{
		ReplicationEventPosition: common.ReplicationEventPosition{Name: "binlog.000001", Pos: 4},
	})
	it.workers = []fetchWorker{oneRowWorker{data: it.data}}
	it.start(ctx)

	recs, err := it.ReadN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1) // delivered and never acked

	<-it.t.Dead() // the only worker finished: the next ReadN waits for the ack

	readCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = it.ReadN(readCtx, 1)
	is.True(errors.Is(err, context.DeadlineExceeded))

	is.NoErr(it.Teardown(ctx)) // bounded wait, proceeds without the ack
}

func TestAckCounter(t *testing.T) {
	t.Run("wait returns at once with nothing pending", func(t *testing.T) {
		is := is.New(t)
		var c ackCounter
		is.NoErr(c.wait(context.Background()))
	})

	t.Run("wait returns when the last ack arrives", func(t *testing.T) {
		is := is.New(t)
		var c ackCounter
		c.add(2)
		is.NoErr(c.done(1))

		errc := make(chan error, 1)
		go func() { errc <- c.wait(context.Background()) }()
		select {
		case <-errc:
			t.Fatal("wait returned with an ack still pending")
		case <-time.After(20 * time.Millisecond):
		}
		is.NoErr(c.done(1))
		select {
		case err := <-errc:
			is.NoErr(err)
		case <-time.After(time.Second):
			t.Fatal("wait did not return after the last ack")
		}
	})

	t.Run("an abandoned wait leaves no goroutine behind", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
		is := is.New(t)
		var c ackCounter
		c.add(1)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		is.True(errors.Is(c.wait(ctx), context.Canceled))
	})

	t.Run("a surplus ack is an error, not a panic, and changes nothing", func(t *testing.T) {
		is := is.New(t)
		var c ackCounter
		is.True(errors.Is(c.done(1), errUnexpectedAck))
		c.add(1)
		is.True(errors.Is(c.done(2), errUnexpectedAck))
		is.NoErr(c.done(1))
		is.NoErr(c.wait(context.Background()))
	})

	t.Run("the counter can be reused after reaching zero", func(t *testing.T) {
		is := is.New(t)
		var c ackCounter
		c.add(1)
		is.NoErr(c.done(1))
		c.add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		is.True(errors.Is(c.wait(ctx), context.DeadlineExceeded))
		is.NoErr(c.done(1))
		is.NoErr(c.wait(context.Background()))
	})

	t.Run("concurrent adds, acks and waits", func(t *testing.T) {
		is := is.New(t)
		var c ackCounter
		const n = 1000
		c.add(n)
		var wg sync.WaitGroup
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
				defer cancel()
				_ = c.wait(ctx)
			}()
		}
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				is.NoErr(c.done(1))
			}()
		}
		wg.Wait()
		is.NoErr(c.wait(context.Background()))
	})
}

// endlessWorker hands the snapshot iterator rows until it is stopped. The
// data channel is unbuffered, so sent counts the rows ReadN received.
type endlessWorker struct {
	data chan fetchData
	sent *atomic.Int64
}

func (w endlessWorker) table() string { return "users" }

func (w endlessWorker) fetchStartEnd(context.Context) (bool, error) { return false, nil }

func (w endlessWorker) run(ctx context.Context) error {
	for i := int64(1); ; i++ {
		select {
		case w.data <- fetchData{
			table:         "users",
			key:           opencdc.StructuredData{"id": i},
			payload:       opencdc.StructuredData{"id": i},
			position:      common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: i, SnapshotEnd: int64(1 << 40)}},
			payloadSchema: &schemaSubjectVersion{subject: "users_payload", version: 1},
			keySchema:     &schemaSubjectVersion{subject: "users_key", version: 1},
		}:
			w.sent.Add(1)
		case <-ctx.Done():
			return nil
		}
	}
}

// Regression test (#224 review): a ReadN whose context ended while it was
// filling a batch dropped the records it had built, but not their acks. The
// count of pending acks then never returned to zero, and the snapshot-to-CDC
// handoff, which waits for it, could not complete. Which select case runs is
// random when both are ready, so the case is repeated until it is hit.
func TestSnapshotIterator_CancelledReadNReleasesDroppedAcks(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	hit := 0
	for attempt := 0; attempt < 2000 && hit < 20; attempt++ {
		it, err := newSnapshotIterator(snapshotIteratorConfig{
			tablePrimaryKeys: common.TableKeys{"users": {"id"}},
			database:         "meroxadb",
			serverID:         "1",
		})
		is.NoErr(err)
		it.setCDCStart(&common.CdcPosition{
			ReplicationEventPosition: common.ReplicationEventPosition{Name: "binlog.000001", Pos: 4},
		})
		var sent atomic.Int64
		it.workers = []fetchWorker{endlessWorker{data: it.data, sent: &sent}}
		it.start(ctx)

		// One record first, so the worker is running and about to offer the
		// next one when the cancelled ReadN starts.
		first, err := it.ReadN(ctx, 1)
		is.NoErr(err)
		is.NoErr(it.Ack(ctx, first[0].Position))
		sent.Store(0)

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		recs, err := it.ReadN(cancelled, 50)
		it.acks.mu.Lock()
		pending := it.acks.pending
		it.acks.mu.Unlock()
		// Stop the worker before reading sent: a send in flight completes
		// only with a receive, and nothing receives now.
		it.t.Kill(nil)
		_ = it.t.Wait()

		if err != nil {
			is.Equal(len(recs), 0)
			if pending != 0 {
				t.Fatalf("attempt %d: ReadN dropped its records but left %d acks pending", attempt, pending)
			}
		} else {
			is.Equal(pending, len(recs)) // delivered, waiting for their acks
		}
		if err != nil && sent.Load() > 0 {
			hit++ // records were received, then dropped on the cancelled context
		}
	}
	is.True(hit > 0) // the cancelled fill path ran at least once
}
