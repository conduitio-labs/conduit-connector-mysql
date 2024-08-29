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
	"strings"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/csync"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/jmoiron/sqlx"
	"gopkg.in/tomb.v2"
)

var ErrSnapshotIteratorDone = errors.New("snapshot complete")

type snapshotKey struct {
	Key   common.PrimaryKeyName `json:"key"`
	Value any                   `json:"value"`
}

func (key snapshotKey) ToSDKData() opencdc.Data {
	return opencdc.StructuredData{string(key.Key): key.Value}
}

type (
	// fetchData is the data that is fetched from a table row. As the iterator
	// fetches rows from multiple tables, reading records from one table affects the
	// position of records of other tables. Each table is fetched concurrently, so
	// in order to prevent data races fetchData builds records within the snapshot
	// iterator itself.
	fetchData struct {
		key      snapshotKey
		table    common.TableName
		payload  opencdc.StructuredData
		position common.TablePosition
	}
	snapshotIterator struct {
		t                *tomb.Tomb
		data             chan fetchData
		acks             csync.WaitGroup
		cdcStartPosition common.CdcPosition
		lastPosition     common.SnapshotPosition
		config           snapshotIteratorConfig
	}
	snapshotIteratorConfig struct {
		getMasterPos  func() (mysql.Position, error)
		db            *sqlx.DB
		tableKeys     common.TableKeys
		fetchSize     int
		startPosition *common.SnapshotPosition
		database      string
		tables        []string
		serverID      common.ServerID
	}
)

func (config *snapshotIteratorConfig) validate() error {
	if config.startPosition == nil {
		config.startPosition = &common.SnapshotPosition{
			Snapshots: map[common.TableName]common.TablePosition{},
		}
	}

	if config.fetchSize == 0 {
		config.fetchSize = common.DefaultFetchSize
	}

	if config.database == "" {
		return fmt.Errorf("database is required")
	}
	if len(config.tables) == 0 {
		return fmt.Errorf("tables is required")
	}

	if config.getMasterPos == nil {
		return fmt.Errorf("getMasterPos mandatory")
	}

	return nil
}

func newSnapshotIterator(
	ctx context.Context,
	config snapshotIteratorConfig,
) (*snapshotIterator, error) {
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

	_, err := config.db.ExecContext(ctx, "LOCK TABLES "+strings.Join(config.tables, " READ, ")+" READ")
	if err != nil {
		return nil, fmt.Errorf("failed to lock tables: %w", err)
	}

	workers := make([]*fetchWorker, 0, len(config.tableKeys))
	for table, primaryKey := range config.tableKeys {
		worker := newFetchWorker(iterator.config.db, iterator.data, fetchWorkerConfig{
			lastPosition: iterator.lastPosition,
			table:        table,
			fetchSize:    iterator.config.fetchSize,
			primaryKey:   primaryKey,
		})
		if err := worker.obtainTx(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch transaction for worker: %w", err)
		}

		workers = append(workers, worker)
	}

	masterPos, err := config.getMasterPos()
	if err != nil {
		// cleanup all worker transactions
		for _, worker := range workers {
			err = errors.Join(err, worker.tx.Rollback())
		}

		return nil, fmt.Errorf("failed to get mysql master position after acquiring locks: %w", err)
	}

	iterator.cdcStartPosition = common.CdcPosition{
		Name: masterPos.Name,
		Pos:  masterPos.Pos,
	}

	for _, worker := range workers {
		iterator.t.Go(func() error {
			ctx := iterator.t.Context(ctx)
			return worker.run(ctx)
		})
	}

	return iterator, nil
}

func (s *snapshotIterator) Next(ctx context.Context) (rec opencdc.Record, err error) {
	select {
	case <-ctx.Done():
		//nolint:wrapcheck // no need to wrap canceled error
		return rec, ctx.Err()
	case <-s.t.Dead():
		if err := s.t.Err(); err != nil && !errors.Is(err, ErrSnapshotIteratorDone) {
			return rec, fmt.Errorf(
				"cannot stop snapshot mode, fetchers exited unexpectedly: %w", err)
		}
		if err := s.acks.Wait(ctx); err != nil {
			return rec, fmt.Errorf("failed to wait for acks on snapshot iterator done: %w", err)
		}

		return rec, ErrSnapshotIteratorDone
	case data := <-s.data:
		s.acks.Add(1)
		return s.buildRecord(data), nil
	}
}

func (s *snapshotIterator) Ack(context.Context, opencdc.Position) error {
	s.acks.Done()
	return nil
}

func (s *snapshotIterator) Teardown(ctx context.Context) error {
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

	sdk.Logger(ctx).Info().Msg("all workers done")

	if _, err := s.config.db.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		return fmt.Errorf("failed to unlock tables while tearing down snapshot iterator: %w", err)
	}

	if s.config.db != nil {
		if err := s.config.db.Close(); err != nil {
			return fmt.Errorf("failed to close database: %w", err)
		}
	}

	sdk.Logger(ctx).Info().Msg("teared down snapshot iterator")

	return nil
}

func (s *snapshotIterator) buildRecord(d fetchData) opencdc.Record {
	s.lastPosition.Snapshots[d.table] = d.position

	pos := s.lastPosition.ToSDKPosition()
	metadata := make(opencdc.Metadata)
	metadata.SetCollection(string(d.table))
	metadata[common.ServerIDKey] = string(s.config.serverID)

	key := d.key.ToSDKData()

	return sdk.Util.Source.NewRecordSnapshot(pos, metadata, key, d.payload)
}
