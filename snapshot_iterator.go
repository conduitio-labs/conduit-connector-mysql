package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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

type Position struct {
	Snapshots SnapshotPositions `json:"snapshots,omitempty"`
}

func (p Position) ToSDKPosition() sdk.Position {
	v, err := json.Marshal(p)
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

type SnapshotPositions map[string]SnapshotPosition

type SnapshotPosition struct {
	LastRead    int64 `json:"last_read"`
	SnapshotEnd int64 `json:"snapshot_end"`
}

type SnapshotKey struct {
	Table string `json:"table"`
	Key   string `json:"key"`
	Value int    `json:"value"`
}

func (key SnapshotKey) ToSDKData() sdk.Data {
	bs, err := json.Marshal(key)
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}

	return sdk.RawData(bs)
}

type snapshotIterator struct {
	db     *sqlx.DB
	tables []string

	lastPosition Position

	acks csync.WaitGroup
	t    *tomb.Tomb

	data chan FetchData
}

type snapshotIteratorConfig struct {
	StartPosition Position
	Database      string
	Tables        []string
}

func newSnapshotIterator(
	ctx context.Context,
	db *sqlx.DB,
	config snapshotIteratorConfig,
) (Iterator, error) {
	t, ctx := tomb.WithContext(ctx)

	tableKeys, err := getTableKeys(db, config.Database, config.Tables)
	if err != nil {
		return nil, fmt.Errorf("failed to get table keys: %w", err)
	}

	it := &snapshotIterator{
		db:           db,
		tables:       config.Tables,
		lastPosition: config.StartPosition,
		acks:         csync.WaitGroup{},
		t:            t,
		data:         make(chan FetchData),
	}

	for _, table := range it.tables {
		key, ok := tableKeys[table]
		if !ok {
			return nil, fmt.Errorf("table %q not found in table keys", table)
		}

		worker := NewFetchWorker(db, it.data, FetchConfig{
			Table:         table,
			Key:           key,
			StartPosition: it.lastPosition,
		})

		t.Go(func() error {
			//nolint:staticcheck // This is the correct usage of tomb.Context
			sdk.Logger(ctx).Info().Msgf("starting fetcher for table %q", table)

			if err := worker.Run(ctx); err != nil {
				return fmt.Errorf("fetcher for table %q exited: %w", table, err)
			}
			return nil
		})
	}

	go func() {
		<-it.t.Dead()
		close(it.data)
	}()

	return it, nil
}

func (s *snapshotIterator) Next(ctx context.Context) (rec sdk.Record, err error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	case d, ok := <-s.data:
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
		return s.buildRecord(d), nil
	}
}

func (s *snapshotIterator) Ack(ctx context.Context, pos sdk.Position) error {
	s.acks.Done()
	return nil
}

func (s *snapshotIterator) Teardown(ctx context.Context) error {
	if s.t != nil {
		s.t.Kill(errors.New("tearing down snapshot iterator"))
	}

	return nil
}

func (s *snapshotIterator) buildRecord(d FetchData) sdk.Record {
	s.lastPosition.Snapshots[d.Table] = d.Position

	pos := s.lastPosition.ToSDKPosition()
	metadata := make(sdk.Metadata)
	metadata["postgres.table"] = d.Table
	key := d.Key.ToSDKData()

	return sdk.Util.Source.NewRecordSnapshot(pos, metadata, key, d.Payload)
}

type FetchConfig struct {
	Table         string
	Key           string
	FetchSize     int
	StartPosition Position
	SnapshotEnd   int
	LastRead      int
}

func NewFetchWorker(db *sqlx.DB, out chan<- FetchData, c FetchConfig) *FetchWorker {
	f := &FetchWorker{
		conf: c,
		db:   db,
		out:  out,
	}

	if f.conf.FetchSize == 0 {
		f.conf.FetchSize = defaultFetchSize
	}

	return f
}

type FetchWorker struct {
	conf FetchConfig
	db   *sqlx.DB
	out  chan<- FetchData
}

type FetchData struct {
	Key      SnapshotKey
	Payload  sdk.StructuredData
	Position SnapshotPosition
	Table    string
}

func (f *FetchWorker) Run(ctx context.Context) error {
	start := time.Now().UTC()

	tx, err := f.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to start tx: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			sdk.Logger(ctx).Err(err).Msg("error on tx rollback")
		}
	}()

	if err := f.updateSnapshotEnd(ctx, tx); err != nil {
		return fmt.Errorf("failed to update fetch limit: %w", err)
	}
	sdk.Logger(ctx).Trace().Msgf("fetch limit updated to %d", f.conf.SnapshotEnd)

	if err := f.fetch(ctx, tx); err != nil {
		return fmt.Errorf("failed to fetch rows: %w", err)
	}

	sdk.Logger(ctx).Trace().Msgf(
		"snapshot completed for table %q, elapsed time: %v",
		f.conf.Table, time.Since(start),
	)
	return nil
}

func (f *FetchWorker) updateSnapshotEnd(ctx context.Context, tx *sqlx.Tx) error {
	if f.conf.SnapshotEnd > 0 {
		return nil
	}

	var maxValueRow struct {
		MaxValue *int `db:"max_value"`
	}

	query := fmt.Sprintf("SELECT MAX(%s) as max_value FROM %s", f.conf.Key, f.conf.Table)
	row := tx.QueryRowxContext(ctx, query)
	if err := row.StructScan(&maxValueRow); err != nil {
		return fmt.Errorf("failed to get max value: %w", err)
	}

	if err := row.Err(); err != nil {
		return fmt.Errorf("failed to get max value: %w", err)
	}

	if maxValueRow.MaxValue == nil {
		// table is empty
		f.conf.SnapshotEnd = 0
	} else {
		f.conf.SnapshotEnd = *maxValueRow.MaxValue
	}

	return nil
}

func (f *FetchWorker) fetch(ctx context.Context, tx *sqlx.Tx) error {
	query := fmt.Sprintf(`
		SELECT *
		FROM %s
		WHERE %s > ? AND %s <= ?
		ORDER BY %s LIMIT %d
	`,
		f.conf.Table,
		f.conf.Key, f.conf.Key,
		f.conf.Key, f.conf.FetchSize,
	)

	rows, err := tx.QueryContext(ctx, query, f.conf.LastRead, f.conf.SnapshotEnd)
	if err != nil {
		return fmt.Errorf("failed to query rows: %w", err)
	}
	defer rows.Close()

	fields, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	for rows.Next() {
		values := make([]any, len(fields))
		valuePtrs := make([]any, len(fields))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		data := f.buildFetchData(ctx, fields, values)

		if err := f.send(ctx, data); err != nil {
			return fmt.Errorf("failed to send record: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read rows: %w", err)
	}

	return nil
}

func (f *FetchWorker) buildFetchData(ctx context.Context, fields []string, values []any) FetchData {
	payload := make(sdk.StructuredData)
	var lastRead int

	for i, field := range fields {
		payload[field] = values[i]
	}

	if val, ok := payload[f.conf.Key]; ok {
		if lastRead, ok = val.(int); !ok {
			sdk.Logger(ctx).Error().Msgf("key %s not found in payload", f.conf.Key)
		}
	} else {
		lastRead = 0
	}

	key := SnapshotKey{
		Table: f.conf.Table,
		Key:   f.conf.Key,
		Value: lastRead,
	}

	f.conf.LastRead = lastRead

	return FetchData{
		Key:     key,
		Payload: payload,
		Table:   f.conf.Table,
	}
}

func (f *FetchWorker) send(ctx context.Context, data FetchData) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case f.out <- data:
		return nil
	}
}

func getTableKeys(db *sqlx.DB, database string, tables []string) (map[string]string, error) {
	primaryKeys := make(map[string]string)

	var formattedTables []string
	for _, table := range tables {
		formattedTables = append(formattedTables, fmt.Sprintf("'%s'", table))
	}
	tableNameIn := strings.Join(formattedTables, ",")

	type Row struct {
		Column_Name string `db:"COLUMN_NAME"`
		Table_Name  string `db:"TABLE_NAME"`
	}

	var rows []Row
	query := fmt.Sprintf(`
		SELECT TABLE_NAME, COLUMN_NAME 
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
		WHERE 
			CONSTRAINT_NAME = 'PRIMARY' 
			AND TABLE_SCHEMA = ?
			AND TABLE_NAME IN (%s);
	`, tableNameIn)
	err := db.Select(&rows, query, database)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary keys from tables: %w", err)
	}

	for _, row := range rows {
		primaryKeys[row.Table_Name] = row.Column_Name
	}

	for _, table := range tables {
		if _, ok := primaryKeys[table]; !ok {
			return nil, fmt.Errorf("table %q has no primary key", table)
		}
	}

	return primaryKeys, nil
}
