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

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"gopkg.in/tomb.v2"
)

var ErrSnapshotIteratorDone = errors.New("snapshot complete")

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

	if err := s.acks.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for snapshot acks: %w", err)
	}

	// waiting for the workers to finish will allow us to have an easier time
	// debugging goroutine leaks.
	_ = s.t.Wait()

	sdk.Logger(ctx).Info().Msg("all workers done, teared down snapshot iterator")

	return nil
}

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
