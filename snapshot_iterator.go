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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"gopkg.in/tomb.v2"
)

var ErrSnapshotIteratorDone = errors.New("snapshot complete")

const defaultFetchSize = 50000

type snapshotKey struct {
	Key   common.PrimaryKeyName `json:"key"`
	Value any                   `json:"value"`
}

func (key snapshotKey) ToSDKData() sdk.Data {
	return sdk.StructuredData{string(key.Key): key.Value}
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
		payload  sdk.StructuredData
		position common.TablePosition
	}
	snapshotIterator struct {
		t            *tomb.Tomb
		data         chan fetchData
		acks         csync.WaitGroup
		lastPosition common.SnapshotPosition
		config       snapshotIteratorConfig
	}
	snapshotIteratorConfig struct {
		db            *sqlx.DB
		tableKeys     common.TableKeys
		fetchSize     int
		startPosition *common.SnapshotPosition
		database      string
		tables        []string
		serverID      common.ServerID
	}
)

func (config *snapshotIteratorConfig) init() error {
	if config.startPosition == nil {
		config.startPosition = &common.SnapshotPosition{
			Snapshots: map[common.TableName]common.TablePosition{},
		}
	}

	if config.fetchSize == 0 {
		config.fetchSize = defaultFetchSize
	}

	if config.database == "" {
		return fmt.Errorf("database is required")
	}
	if len(config.tables) == 0 {
		return fmt.Errorf("tables is required")
	}

	return nil
}

func newSnapshotIterator(
	ctx context.Context,
	config snapshotIteratorConfig,
) (common.Iterator, error) {
	if err := config.init(); err != nil {
		return nil, fmt.Errorf("invalid snapshot iterator config: %w", err)
	}

	// start position is mutable, so in order to avoid unexpected behaviour in
	// tests we clone it.
	lastPosition := config.startPosition.Clone()

	iterator := &snapshotIterator{
		t:            &tomb.Tomb{},
		data:         make(chan fetchData),
		acks:         csync.WaitGroup{},
		config:       config,
		lastPosition: lastPosition,
	}

	for table, primaryKey := range config.tableKeys {
		worker := newFetchWorker(iterator.config.db, iterator.data, fetchWorkerConfig{
			lastPosition: iterator.lastPosition,
			table:        table,
			fetchSize:    iterator.config.fetchSize,
			primaryKey:   primaryKey,
		})
		iterator.t.Go(func() error {
			return worker.run(ctx)
		})
	}

	return iterator, nil
}

func (s *snapshotIterator) Next(ctx context.Context) (rec sdk.Record, err error) {
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

func (s *snapshotIterator) Ack(context.Context, sdk.Position) error {
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

	return nil
}

func (s *snapshotIterator) buildRecord(d fetchData) sdk.Record {
	s.lastPosition.Snapshots[d.table] = d.position

	pos := s.lastPosition.ToSDKPosition()
	metadata := make(sdk.Metadata)
	metadata.SetCollection(string(d.table))
	metadata[common.ServerIDKey] = string(s.config.serverID)

	key := d.key.ToSDKData()

	return sdk.Util.Source.NewRecordSnapshot(pos, metadata, key, d.payload)
}

func getPrimaryKey(db *sqlx.DB, database, table string) (common.PrimaryKeyName, error) {
	var primaryKey struct {
		ColumnName common.PrimaryKeyName `db:"COLUMN_NAME"`
	}

	row := db.QueryRowx(`
		SELECT COLUMN_NAME 
		FROM information_schema.key_column_usage 
		WHERE 
			constraint_name = 'PRIMARY' 
			AND table_schema = ?
			AND table_name = ?
	`, database, table)

	if err := row.StructScan(&primaryKey); err != nil {
		return "", fmt.Errorf("failed to get primary key from table %s: %w", table, err)
	}
	if err := row.Err(); err != nil {
		return "", fmt.Errorf("failed to scan primary key from table %s: %w", table, err)
	}

	return primaryKey.ColumnName, nil
}

func getTableKeys(db *sqlx.DB, database string, tables []string) (common.TableKeys, error) {
	tableKeys := make(common.TableKeys)

	for _, table := range tables {
		primaryKey, err := getPrimaryKey(db, database, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key for table %q: %w", table, err)
		}

		tableKeys[common.TableName(table)] = primaryKey
	}

	return tableKeys, nil
}
