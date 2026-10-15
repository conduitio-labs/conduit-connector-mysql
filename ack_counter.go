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
)

// errUnexpectedAck is returned for an ack that has no delivered record left to
// match.
var errUnexpectedAck = errors.New("received an ack for a snapshot record that was not delivered, or was already acked")

// ackCounter counts snapshot records that were delivered and not yet acked.
//
// It replaces a csync.WaitGroup, whose context-aware Wait starts a goroutine
// that blocks on sync.WaitGroup.Wait until the counter reaches zero. When the
// wait gave up (Teardown's bounded wait, or a cancelled ReadN), that goroutine
// stayed blocked for as long as the acks never arrived, which after a stop is
// forever: one leaked goroutine per abandoned wait. wait here selects on a
// channel instead, so giving up leaves nothing behind.
//
// The zero value is ready to use. All methods are safe for concurrent use: the
// SDK calls Ack from its own goroutine while ReadN runs.
type ackCounter struct {
	mu      sync.Mutex
	pending int
	// zero is closed when pending drops to zero. nil while pending is zero
	// (wait does not need it then); replaced on every 0 -> 1 transition.
	zero chan struct{}
}

// add records n delivered records whose acks are now expected.
func (c *ackCounter) add(n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending == 0 {
		c.zero = make(chan struct{})
	}
	c.pending += n
}

// done records n acks. More acks than delivered records is an error instead of
// a panic (sync.WaitGroup panics on a negative counter); the counter is left
// unchanged in that case.
func (c *ackCounter) done(n int) error {
	if n <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if n > c.pending {
		return errUnexpectedAck
	}
	c.pending -= n
	if c.pending == 0 {
		close(c.zero)
		c.zero = nil
	}
	return nil
}

// wait blocks until every delivered record is acked or ctx is done, and returns
// ctx.Err() in the latter case. It starts no goroutine.
func (c *ackCounter) wait(ctx context.Context) error {
	c.mu.Lock()
	if c.pending == 0 {
		c.mu.Unlock()
		return nil
	}
	zero := c.zero
	c.mu.Unlock()

	select {
	case <-zero:
		return nil
	case <-ctx.Done():
		return ctx.Err() //nolint:wrapcheck // callers match on context errors
	}
}
