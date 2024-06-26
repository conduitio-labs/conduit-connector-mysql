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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/conduitio/conduit-commons/csync"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"gopkg.in/tomb.v2"
)

var ErrIteratorDone = errors.New("snapshot complete")

const defaultFetchSize = 50000

// Iterator is an object that can iterate over a queue of records.
type Iterator interface {
	// Next takes and returns the next record from the queue. Next is allowed to
	// block until either a record is available or the context gets canceled.
	Next(context.Context) (sdk.Record, error)
	// Ack signals that a record at a specific position was successfully
	// processed.
	Ack(context.Context, sdk.Position) error
	// Teardown attempts to gracefully teardown the iterator.
	Teardown(context.Context) error
}

type position struct {
	Snapshots snapshotPositions `json:"snapshots,omitempty"`
}

func (p position) ToSDKPosition() sdk.Position {
	v, err := json.Marshal(p)
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

func (p position) Clone() position {
	var newPosition position
	newPosition.Snapshots = make(map[tableName]snapshotPosition)
	for k, v := range p.Snapshots {
		newPosition.Snapshots[k] = v
	}
	return newPosition
}

func parseSDKPosition(p sdk.Position) (position, error) {
	var pos position
	if err := json.Unmarshal(p, &pos); err != nil {
		return position{}, fmt.Errorf("failed to parse position: %w", err)
	}
	return pos, nil
}

type snapshotPositions map[tableName]snapshotPosition

type snapshotPosition struct {
	LastRead    int `json:"last_read"`
	SnapshotEnd int `json:"snapshot_end"`
}

type snapshotKey struct {
	Table tableName      `json:"table"`
	Key   primaryKeyName `json:"key"`
	Value int            `json:"value"`
}

func (key snapshotKey) ToSDKData() sdk.Data {
	bs, err := json.Marshal(key)
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}

	return sdk.RawData(bs)
}

type (
	// fetchData is the data that is fetched from a table row. As the iterator
	// fetches rows from multiple tables, reading records from one table affects the
	// position of records of other tables. Each table is fetched concurrently, so
	// in order to prevent data races fetchData builds records within the snapshot
	// iterator itself.
	fetchData struct {
		key      snapshotKey
		payload  sdk.StructuredData
		position snapshotPosition
	}
	snapshotIterator struct {
		db           *sqlx.DB
		t            *tomb.Tomb
		data         chan fetchData
		acks         csync.WaitGroup
		fetchSize    int
		lastPosition position
	}
	snapshotIteratorConfig struct {
		db            *sqlx.DB
		fetchSize     int
		startPosition position
		database      string
		tables        []string
	}
)

type (
	primaryKeyName string
	tableName      string
	TableKeys      map[tableName]primaryKeyName
)

func (config *snapshotIteratorConfig) init() (TableKeys, error) {
	if config.startPosition.Snapshots == nil {
		config.startPosition.Snapshots = make(map[tableName]snapshotPosition)
	}

	if config.fetchSize == 0 {
		config.fetchSize = defaultFetchSize
	}

	if config.database == "" {
		return nil, fmt.Errorf("database is required")
	}
	if len(config.tables) == 0 {
		return nil, fmt.Errorf("tables is required")
	}

	tableKeys := make(TableKeys)

	for _, table := range config.tables {
		primaryKey, err := getPrimaryKey(config.db, config.database, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key for table %q: %w", table, err)
		}

		tableKeys[tableName(table)] = primaryKey
	}

	return tableKeys, nil
}

func newSnapshotIterator(
	ctx context.Context,
	config snapshotIteratorConfig,
) (Iterator, error) {
	tableKeys, err := config.init()
	if err != nil {
		return nil, fmt.Errorf("invalid snapshot iterator config: %w", err)
	}

	// start position is mutable, so in order to avoid unexpected behaviour in
	// tests we clone it.
	lastPosition := config.startPosition.Clone()

	iterator := &snapshotIterator{
		db:           config.db,
		t:            &tomb.Tomb{},
		data:         make(chan fetchData),
		acks:         csync.WaitGroup{},
		fetchSize:    config.fetchSize,
		lastPosition: lastPosition,
	}

	for table, primaryKey := range tableKeys {
		worker := newFetchWorker(iterator.db, iterator.data, fetchWorkerConfig{
			lastPosition: iterator.lastPosition,
			table:        table,
			fetchSize:    iterator.fetchSize,
			primaryKey:   primaryKey,
		})
		iterator.t.Go(func() error {
			return worker.run(ctx)
		})
	}

	// close the data channel when all tomb goroutines are done, which will happen:
	// - when all records are fetched from all tables
	// - when the iterator teardown method is called
	go func() {
		<-iterator.t.Dead()
		close(iterator.data)
	}()

	return iterator, nil
}

func (s *snapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, fmt.Errorf("context cancelled: %w", ctx.Err())
	case data, ok := <-s.data:
		if !ok { // closed
			if err := s.t.Err(); err != nil {
				return sdk.Record{}, fmt.Errorf("fetchers exited unexpectedly: %w", err)
			}
			if err := s.acks.Wait(ctx); err != nil {
				return sdk.Record{}, fmt.Errorf("failed to wait for acks: %w", err)
			}
			return sdk.Record{}, ErrIteratorDone
		}

		s.acks.Add(1)
		return s.buildRecord(data), nil
	}
}

func (s *snapshotIterator) Ack(_ context.Context, _ sdk.Position) error {
	s.acks.Done()
	return nil
}

func (s *snapshotIterator) Teardown(_ context.Context) error {
	if s.t != nil {
		s.t.Kill(errors.New("tearing down snapshot iterator"))
	}

	return nil
}

func (s *snapshotIterator) buildRecord(d fetchData) sdk.Record {
	s.lastPosition.Snapshots[d.key.Table] = d.position

	pos := s.lastPosition.ToSDKPosition()
	metadata := make(sdk.Metadata)
	metadata["postgres.table"] = string(d.key.Table)
	key := d.key.ToSDKData()

	return sdk.Util.Source.NewRecordSnapshot(pos, metadata, key, d.payload)
}

func getPrimaryKey(db *sqlx.DB, database, table string) (primaryKeyName, error) {
	var primaryKey struct {
		ColumnName primaryKeyName `db:"COLUMN_NAME"`
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
