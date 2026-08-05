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
	"errors"
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"gopkg.in/tomb.v2"
)

var ErrSnapshotIteratorDone = errors.New("snapshot complete")

// DefaultTeardownAckTimeout bounds how long Teardown waits for outstanding
// snapshot acks before proceeding with teardown regardless.
//
// It exists because Teardown previously waited on those acks with only the
// caller's context for liveness, and the Conduit SDK calls a plugin's Teardown
// with a context carrying NO deadline (sourcePluginAdapter.Teardown). So a
// boundary ack that never arrived meant Teardown never returned: a graceful
// stop could not complete and the operator had to kill -9 a pipeline that
// reported itself as stopping. That is invariant 7 (SIGTERM drains and
// checkpoints before exit) violated, and it was reproducible — see
// handoff_ack_gate_integration_test.go.
//
// Proceeding early is SAFE here, and safer than waiting forever. Teardown does
// not advance the source position: whatever was last acked is what the engine
// persisted. Snapshot rows whose acks never arrived are simply re-read on the
// next run, so the cost of this bound is at worst duplicate delivery, never a
// gap (invariant 3 holds). The cost of NOT having it is an unbounded hang.
//
// 10s deliberately mirrors conduit's own DefaultTeardownFlushTimeout
// (pkg/connector/source.go), which bounds the equivalent wait on the engine
// side for the same reason: comfortably shorter than a typical Kubernetes
// termination grace period, so a stuck ack degrades to a slightly-slow clean
// shutdown rather than a SIGKILL.
//
// A var, not a const, solely so tests can lower it. Production code must never
// reassign it.
var DefaultTeardownAckTimeout = 10 * time.Second

type (
	// fetchData is the data that is fetched from a table row. As the iterator
	// fetches rows from multiple tables, reading records from one table affects the
	// position of records of other tables. Each table is fetched concurrently, so
	// in order to prevent data races fetchData builds records within the snapshot
	// iterator itself.
	fetchData struct {
		table    string
		key      opencdc.Data
		payload  opencdc.StructuredData
		position common.TablePosition

		payloadSchema *schemaSubjectVersion

		// keySchema might be nil, as fetchWorkerByLimit doesn't have any key
		keySchema *schemaSubjectVersion
	}
	snapshotIterator struct {
		t            *tomb.Tomb
		data         chan fetchData
		acks         csync.WaitGroup
		lastPosition common.SnapshotPosition
		workers      []fetchWorker
		config       snapshotIteratorConfig
	}
	snapshotIteratorConfig struct {
		db               *sqlx.DB
		tablePrimaryKeys common.TableKeys
		fetchSize        uint64
		startPosition    *common.SnapshotPosition
		database         string
		serverID         string
	}
)

func (config *snapshotIteratorConfig) validate() error {
	if config.startPosition == nil {
		config.startPosition = &common.SnapshotPosition{
			Snapshots: common.SnapshotPositions{},
		}
	}

	if config.fetchSize == 0 {
		config.fetchSize = DefaultFetchSize
	}

	if config.database == "" {
		return fmt.Errorf("database is required")
	}
	if len(config.tablePrimaryKeys) == 0 {
		return fmt.Errorf("tablePrimaryKeys is required")
	}

	return nil
}

func newSnapshotIterator(config snapshotIteratorConfig) (*snapshotIterator, error) {
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid snapshot iterator config: %w", err)
	}

	// Start position is mutable, so in order to avoid unexpected behaviour in
	// tests we clone it.
	lastPosition := config.startPosition.Clone()

	iterator := &snapshotIterator{
		t:            &tomb.Tomb{},
		data:         make(chan fetchData),
		acks:         csync.WaitGroup{},
		config:       config,
		lastPosition: lastPosition,
	}

	return iterator, nil
}

// setupWorkers collects and sets up the snapshot fetch workers. It is separated
// from the start method so that we can lock and unlock the given tables without
// starting up the workers.
func (s *snapshotIterator) setupWorkers(ctx context.Context) error {
	for table, primaryKeys := range s.config.tablePrimaryKeys {
		worker := newFetchWorker(ctx, fetchWorkerConfig{
			// the snapshot worker will update the last position, so we need to
			// clone it to avoid dataraces
			lastPosition: s.lastPosition.Clone(),
			table:        table,
			fetchSize:    s.config.fetchSize,
			primaryKeys:  primaryKeys,
			db:           s.config.db,
			data:         s.data,
		})

		isTableEmpty, err := worker.fetchStartEnd(ctx)
		if err != nil {
			return fmt.Errorf("failed to start worker: %w", err)
		} else if isTableEmpty {
			sdk.Logger(ctx).Info().Msgf("table %s is empty, skipping...", table)
			continue
		}

		s.workers = append(s.workers, worker)
	}

	return nil
}

// setCDCStart stamps the CDC start position (P0) that buildRecord will attach to
// every subsequently emitted snapshot record as SnapshotPosition.CDCStart. It
// must be called before start(ctx) is invoked (see the Invariant 3 comment on
// newCombinedIterator and buildRecord): P0 must be durable on the very first
// snapshot record so a mid-snapshot restart can resume CDC from it instead of a
// fresh, later master position.
func (s *snapshotIterator) setCDCStart(p0 *common.CdcPosition) {
	if p0 == nil {
		return
	}
	// Copy so lastPosition owns its value rather than aliasing the caller's
	// (e.g. the cdcIterator's live position field).
	cdcStart := *p0
	s.lastPosition.CDCStart = &cdcStart
}

func (s *snapshotIterator) start(ctx context.Context) {
	for _, worker := range s.workers {
		s.t.Go(func() error {
			ctx := s.t.Context(ctx)
			return worker.run(ctx)
		})

		sdk.Logger(ctx).Info().Msgf("started worker for table %s", worker.table())
	}
}

func (s *snapshotIterator) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	if len(s.workers) == 0 {
		return nil, ErrSnapshotIteratorDone
	}

	var recs []opencdc.Record

	// block until we get at least one record or context is done
	select {
	case <-ctx.Done():
		//nolint:wrapcheck // no need to wrap canceled error
		return nil, ctx.Err()
	case <-s.t.Dead():
		if err := s.t.Err(); err != nil && !errors.Is(err, ErrSnapshotIteratorDone) {
			return nil, fmt.Errorf(
				"cannot stop snapshot mode, fetchers exited unexpectedly: %w", err)
		}
		if err := s.acks.Wait(ctx); err != nil {
			return nil, fmt.Errorf("failed to wait for acks on snapshot iterator done: %w", err)
		}
		return nil, ErrSnapshotIteratorDone
	case data := <-s.data:
		s.acks.Add(1)
		recs = append(recs, s.buildRecord(data))
	}

	// get the remaining n-1 records is available
	for len(recs) < n {
		select {
		case data := <-s.data:
			s.acks.Add(1)
			recs = append(recs, s.buildRecord(data))
		case <-ctx.Done():
			//nolint:wrapcheck // no need to wrap canceled error
			return nil, ctx.Err()
		default:
			// no more data available now
			return recs, nil
		}
	}

	return recs, nil
}

func (s *snapshotIterator) Ack(context.Context, opencdc.Position) error {
	s.acks.Done()
	return nil
}

func (s *snapshotIterator) Teardown(ctx context.Context) error {
	if len(s.workers) == 0 {
		return nil
	}

	s.t.Kill(ErrSnapshotIteratorDone)
	if err := s.t.Err(); err != nil && !errors.Is(err, ErrSnapshotIteratorDone) {
		return fmt.Errorf(
			"cannot teardown snapshot mode, fetchers exited unexpectedly: %w", err)
	}

	// Invariant 7: bound this wait. See DefaultTeardownAckTimeout for why an
	// unbounded wait here made graceful shutdown impossible, and why proceeding
	// early is safe (unacked snapshot rows replay; teardown advances no
	// position). Derived from ctx so an already-cancelled caller still short-
	// circuits immediately rather than waiting out the full bound.
	ackCtx, cancelAcks := context.WithTimeout(ctx, DefaultTeardownAckTimeout)
	defer cancelAcks()

	if err := s.acks.Wait(ackCtx); err != nil {
		// Deliberately NOT returned as an error. Teardown's job is to release
		// resources; failing it on un-arrived acks leaves the caller with a
		// half-torn-down connector and no better options. Log loudly instead so
		// the condition is visible, then continue.
		sdk.Logger(ctx).Warn().Err(err).
			Dur("timeout", DefaultTeardownAckTimeout).
			Msg("snapshot acks did not arrive before teardown; proceeding. " +
				"Unacked snapshot rows will be re-read on the next run (at-least-once " +
				"preserved). Persistent occurrences mean acks are not reaching the " +
				"connector — check for a stalled destination or a paused pipeline")
	}

	// waiting for the workers to finish will allow us to have an easier time
	// debugging goroutine leaks.
	_ = s.t.Wait()

	sdk.Logger(ctx).Info().Msg("all workers done, teared down snapshot iterator")

	return nil
}

// buildRecord advances lastPosition with the newly fetched row and marshals it
// into the record's position.
//
// Invariant 3: persist the CDC start position (P0) on every snapshot record so a
// mid-snapshot restart resumes CDC from P0. Resuming from a fresh master position
// would silently drop writes to already-copied rows in (P0, P1]. lastPosition.CDCStart
// is set once by setCDCStart, before start(ctx) runs and therefore before this method
// is ever called (see newCombinedIterator), so every record built here - including
// the very first one - carries it via ToSDKPosition.
func (s *snapshotIterator) buildRecord(d fetchData) opencdc.Record {
	s.lastPosition.Snapshots[d.table] = d.position

	pos := s.lastPosition.ToSDKPosition()
	metadata := make(opencdc.Metadata)
	metadata.SetCollection(d.table)
	metadata[common.ServerIDKey] = s.config.serverID

	rec := sdk.Util.Source.NewRecordSnapshot(pos, metadata, d.key, d.payload)

	rec.Metadata.SetPayloadSchemaSubject(d.payloadSchema.subject)
	rec.Metadata.SetPayloadSchemaVersion(d.payloadSchema.version)

	if d.keySchema != nil {
		rec.Metadata.SetKeySchemaSubject(d.keySchema.subject)
		rec.Metadata.SetKeySchemaVersion(d.keySchema.version)
	}

	return rec
}
