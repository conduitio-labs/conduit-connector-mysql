package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/csync"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"gopkg.in/tomb.v2"
)

var ErrIteratorDone = errors.New("snapshot complete")

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

type PositionType int

const (
	PositionTypeInitial PositionType = iota
	PositionTypeSnapshot
	PositionTypeCDC
)

type Position struct {
	Type      PositionType      `json:"type"`
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
	Value int64  `json:"value"`
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
	db     *sql.DB
	tables []string

	lastPosition Position

	acks csync.WaitGroup
	t    *tomb.Tomb

	data chan FetchData
}

type snapshotIteratorConfig struct {
	Position  Position
	Tables    []string
	TableKeys map[string]string
}

func newSnapshotIterator(
	ctx context.Context,
	db *sql.DB,
	config snapshotIteratorConfig,
) (Iterator, error) {
	t, _ := tomb.WithContext(ctx)

	it := &snapshotIterator{
		db:           db,
		tables:       config.Tables,
		lastPosition: config.Position,
		acks:         csync.WaitGroup{},
		t:            t,
		data:         make(chan FetchData),
	}

	for _, table := range it.tables {
		key, ok := config.TableKeys[table]
		if !ok {
			return nil, fmt.Errorf("table %q not found in table keys", table)
		}

		worker := NewFetchWorker(db, it.data, FetchConfig{
			Table:     table,
			Key:       key,
			FetchSize: 1000,
			Position:  it.lastPosition,
		})

		t.Go(func() error {
			//nolint:staticcheck // This is the correct usage of tomb.Context
			ctx := it.t.Context(nil)

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
	// merge this position with latest position
	s.lastPosition.Type = PositionTypeSnapshot
	s.lastPosition.Snapshots[d.Table] = d.Position

	pos := s.lastPosition.ToSDKPosition()
	metadata := make(sdk.Metadata)
	metadata["postgres.table"] = d.Table
	key := d.Key.ToSDKData()

	return sdk.Util.Source.NewRecordSnapshot(pos, metadata, key, d.Payload)
}

const defaultFetchSize = 50000

type FetchConfig struct {
	Table       string
	Key         string
	FetchSize   int
	Position    Position
	SnapshotEnd int
	LastRead    int64
}

func NewFetchWorker(db *sql.DB, out chan<- FetchData, c FetchConfig) *FetchWorker {
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
	db   *sql.DB
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

	tx, err := f.db.BeginTx(ctx, &sql.TxOptions{
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

	if err := f.fetch(ctx, tx); err != nil {
		return fmt.Errorf("failed to fetch rows: %w", err)
	}

	sdk.Logger(ctx).Info().Msgf(
		"snapshot completed for table %q, elapsed time: %v",
		f.conf.Table, time.Since(start),
	)
	return nil
}

func (f *FetchWorker) updateSnapshotEnd(ctx context.Context, tx *sql.Tx) error {
	if f.conf.SnapshotEnd > 0 {
		return nil
	}

	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", f.conf.Key, f.conf.Table)
	if err := tx.QueryRowContext(ctx, query).Scan(&f.conf.SnapshotEnd); err != nil {
		return fmt.Errorf("failed to query max on %q.%q: %w", f.conf.Table, f.conf.Key, err)
	}

	return nil
}

func (f *FetchWorker) fetch(ctx context.Context, tx *sql.Tx) error {
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

	for {
		rows, err := tx.QueryContext(ctx, query, f.conf.LastRead, f.conf.SnapshotEnd)
		if err != nil {
			return fmt.Errorf("failed to query rows: %w", err)
		}

		fields, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("failed to get columns: %w", err)
		}

		var nfetched int
		for rows.Next() {
			values := make([]any, len(fields))
			valuePtrs := make([]any, len(fields))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}

			data := f.buildFetchData(fields, values)
			if err != nil {
				return fmt.Errorf("failed to build fetch data: %w", err)
			}

			if err := f.send(ctx, data); err != nil {
				return fmt.Errorf("failed to send record: %w", err)
			}

			nfetched++
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to read rows: %w", err)
		}
		rows.Close()

		if nfetched == 0 {
			break
		}
	}

	return nil
}

func (f *FetchWorker) buildFetchData(fields []string, values []any) FetchData {

	payload := make(sdk.StructuredData)
	var lastRead int64

	for i, field := range fields {
		payload[field] = values[i]
	}

	if val, ok := payload[f.conf.Key]; ok {
		if lastRead, ok = val.(int64); !ok {
			sdk.Logger(context.Background()).Fatal().Msgf(
				"key %s not found in payload",
				f.conf.Key,
			)
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
