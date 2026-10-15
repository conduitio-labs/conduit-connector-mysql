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
	"database/sql"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

var ErrPrimaryKeyNotFoundInRow = errors.New("primary key not found in row")

type fetchWorker interface {
	// for debug purposes
	table() string

	run(ctx context.Context) error
	fetchStartEnd(ctx context.Context) (tableEmpty bool, err error)
}

type fetchWorkerConfig struct {
	db   *sqlx.DB
	data chan fetchData

	lastPosition common.SnapshotPosition
	table        string
	fetchSize    uint64
	primaryKeys  common.PrimaryKeys
	types        *typeSettings
}

func newFetchWorker(ctx context.Context, config fetchWorkerConfig) fetchWorker {
	// Tables without a key never get here: Open and snapshotIteratorConfig.validate
	// refuse them, because a LIMIT/OFFSET snapshot can neither record its position
	// nor read a consistent order (issue #190 was its teardown hang).
	switch len(config.primaryKeys) {
	case 1:
		tablePosition := config.lastPosition.Snapshots[config.table]
		var singleKeyPosition common.TablePositionSingleKey
		if tablePosition.SingleKey != nil {
			singleKeyPosition = *tablePosition.SingleKey
		} else {
			sdk.Logger(ctx).Warn().
				Str("table", config.table).
				Msg("single key position not found in last position, defaulting to empty")
		}

		return newFetchWorkerByKey(config.primaryKeys[0], singleKeyPosition, config)
	default:
		tablePosition, ok := config.lastPosition.Snapshots[config.table]
		switch {
		case !ok:
			return newFetchWorkerByKeys(fetchWorkerByKeysConfig{
				fetchWorkerConfig: config,
				// intentionally omit lastPosition, we'll use the zero value of it
			})
		case tablePosition.MultipleKey != nil:
			return newFetchWorkerByKeys(fetchWorkerByKeysConfig{
				fetchWorkerConfig: config,
				lastPosition:      *tablePosition.MultipleKey,
			})
		default:
			// The persisted position for this table is not a composite-key
			// position (e.g. the key columns changed). Restart the table from
			// its first row: duplicates, never a gap (invariant 3).
			sdk.Logger(ctx).Warn().
				Str("table", config.table).
				Msg("multiple key position not found in last position, restarting the table snapshot from the beginning")

			return newFetchWorkerByKeys(fetchWorkerByKeysConfig{fetchWorkerConfig: config})
		}
	}
}

// fetchWorkerSingleKey will perform a snapshot using the given primary key as
// the sorting key for fetching rows in chunks.
type fetchWorkerSingleKey struct {
	config        fetchWorkerConfig
	lastPosition  common.TablePositionSingleKey
	payloadSchema *schemaMapper
	keySchema     *schemaMapper
	primaryKey    string

	start, end any
}

func newFetchWorkerByKey(
	primaryKey string, lastPosition common.TablePositionSingleKey,
	config fetchWorkerConfig,
) fetchWorker {
	return &fetchWorkerSingleKey{
		primaryKey:    primaryKey,
		lastPosition:  lastPosition,
		payloadSchema: newSchemaMapper(config.types),
		keySchema:     newSchemaMapper(config.types),
		config:        config,
	}
}

func (w *fetchWorkerSingleKey) table() string {
	return w.config.table
}

func (w *fetchWorkerSingleKey) fetchStartEnd(ctx context.Context) (isTableEmpty bool, err error) {
	row, isEmpty, err := getMinMaxValues(ctx, w.primaryKey, w.config)
	if err != nil {
		return false, err
	} else if isEmpty {
		return true, nil
	}

	w.start = row.MinValue
	lastRead := w.lastPosition.LastRead
	if lastRead != nil {
		w.start = lastRead
	}

	w.end = row.MaxValue

	sdk.Logger(ctx).Debug().
		Any("start", w.start).
		Type("start type", w.start).
		Any("end", w.end).
		Type("end type", w.end).
		Msg("fetched start and end")

	return false, nil
}

func (w *fetchWorkerSingleKey) run(ctx context.Context) (err error) {
	sdk.Logger(ctx).Debug().Msgf("started fetch worker by key for table %q", w.config.table)
	defer sdk.Logger(ctx).Debug().Msgf("finished fetch worker by key for table %q", w.config.table)

	logger := sdk.Logger(ctx).With().Str("table", w.config.table).Logger()

	logger.Debug().
		Any("start", w.start).
		Any("end", w.end).
		Uint64("fetchSize", w.config.fetchSize).
		Msg("fetching rows")

	// If the worker has been given a starting position it means that we have already
	// read the record in that specific position, so we can just exclude it.

	discardFirst := w.lastPosition.LastRead != nil
	chunkStart := w.start
	for {
		logger.Trace().
			Any("chunk start", chunkStart).
			Any("end", w.end).Msg("fetching chunk")

		rowsChunk, err := w.selectRowsChunk(ctx, chunkStart, discardFirst)
		if err != nil {
			return fmt.Errorf("failed to select rows chunk: %w", err)
		}
		discardFirst = true

		for _, row := range rowsChunk.rows {
			logger.Trace().Msgf("fetched row: %+v", row)

			lastRead := row[w.primaryKey]
			if lastRead == nil {
				return ErrPrimaryKeyNotFoundInRow
			}

			data, err := w.buildFetchData(ctx, row, rowsChunk.mysqlAvroCols, lastRead)
			if err != nil {
				return fmt.Errorf("failed to build fetch data: %w", err)
			}

			select {
			case w.config.data <- data:
			case <-ctx.Done():
				return fmt.Errorf(
					"fetch worker context done while waiting for data: %w", ctx.Err(),
				)
			}

			chunkStart = lastRead
		}

		if rowsChunk.foundEnd {
			break
		}
	}

	return nil
}

type rowsChunk struct {
	rows          []map[string]any
	mysqlAvroCols []*avroNamedType
	foundEnd      bool
}

func (w *fetchWorkerSingleKey) selectRowsChunk(
	ctx context.Context, start any, discardFirst bool,
) (_ *rowsChunk, err error) {
	// Invariant 3: bind keys typed; see typeSettings.bindKey.
	start = w.config.types.bindKey(w.config.table, w.primaryKey, start)
	end := w.config.types.bindKey(w.config.table, w.primaryKey, w.end)
	var whereStart any = squirrel.GtOrEq{w.primaryKey: start}
	if discardFirst {
		whereStart = squirrel.Gt{w.primaryKey: start}
	}
	whereEnd := squirrel.LtOrEq{w.primaryKey: end}

	query, args, err := squirrel.
		Select("*").
		From(w.config.table).
		Where(whereStart).Where(whereEnd).
		OrderBy(w.primaryKey).
		Limit(w.config.fetchSize).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	sdk.Logger(ctx).Debug().Str("query", query).Any("args", args).Msg("created query")

	rows, err := w.config.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf("failed to close rows: %w", closeErr)
			err = errors.Join(err, closeErr)
		}
	}()

	chunk := &rowsChunk{}

	chunk.mysqlAvroCols, err = sqlxRowsToAvroCol(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column types: %w", err)
	}

	for rows.Next() {
		row := map[string]any{}
		if err := rows.MapScan(row); err != nil {
			return nil, fmt.Errorf("failed to map scan row: %w", err)
		}

		chunk.rows = append(chunk.rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to close rows: %w", err)
	}

	//nolint:gosec // fetchSize is already checked for being a sane int value
	chunk.foundEnd = len(chunk.rows) < int(w.config.fetchSize)

	return chunk, nil
}

func (w *fetchWorkerSingleKey) buildFetchData(
	ctx context.Context, row map[string]any,
	colTypes []*avroNamedType, lastRead any,
) (fetchData, error) {
	var keyType string
	for _, colType := range colTypes {
		if colType.Name == w.primaryKey {
			keyType = colType.dbType
			break
		}
	}
	position := common.TablePosition{
		SingleKey: &common.TablePositionSingleKey{
			LastRead:    lastRead,
			SnapshotEnd: w.end,
			KeyType:     keyType,
		},
	}

	payloadSubver, err := w.payloadSchema.createPayloadSchema(ctx, w.config.table, colTypes)
	if err != nil {
		return fetchData{}, fmt.Errorf("failed to create payload schema for table %s: %w", w.config.table, err)
	}

	payload := make(opencdc.StructuredData)
	for key, val := range row {
		formatted, err := w.payloadSchema.formatValue(ctx, key, val)
		if err != nil {
			return fetchData{}, err
		}
		payload[key] = formatted
	}

	var keyColType *avroNamedType
	for _, colType := range colTypes {
		if colType.Name == w.primaryKey {
			keyColType = colType
			break
		}
	}
	if keyColType == nil {
		return fetchData{}, fmt.Errorf("failed to find key schema column type for table %s", w.config.table)
	}

	keySubver, err := w.keySchema.createKeySchema(ctx, w.config.table, []*avroNamedType{keyColType})
	if err != nil {
		return fetchData{}, fmt.Errorf("failed to create key schema for table %s: %w", w.config.table, err)
	}

	keyVal, ok := row[w.primaryKey]
	if !ok {
		return fetchData{}, fmt.Errorf("key %s not found in payload", w.primaryKey)
	}
	keyVal, err = w.keySchema.formatValue(ctx, w.primaryKey, keyVal)
	if err != nil {
		return fetchData{}, err
	}

	key := opencdc.StructuredData{w.primaryKey: keyVal}
	return fetchData{
		key:           key,
		table:         w.config.table,
		payload:       payload,
		position:      position,
		payloadSchema: payloadSubver,
		keySchema:     keySubver,
	}, nil
}

type fetchWorkerMultipleKey struct {
	config        fetchWorkerByKeysConfig
	payloadSchema *schemaMapper
	keySchema     *schemaMapper

	start, end common.TablePositionMultipleKey

	// resume is true when start is a real, already-delivered row (the persisted
	// last position) that must be excluded, false when start is only the table
	// minimum and the first chunk must read from the beginning.
	resume bool
}

type fetchWorkerByKeysConfig struct {
	fetchWorkerConfig

	lastPosition common.TablePositionMultipleKey
}

func newFetchWorkerByKeys(config fetchWorkerByKeysConfig) fetchWorker {
	return &fetchWorkerMultipleKey{
		config:        config,
		payloadSchema: newSchemaMapper(config.types),
		keySchema:     newSchemaMapper(config.types),
	}
}

func (w *fetchWorkerMultipleKey) table() string {
	return w.config.table
}

func (w *fetchWorkerMultipleKey) fetchStartEnd(ctx context.Context) (tableEmpty bool, err error) {
	for _, primaryKey := range w.config.primaryKeys {
		scanned, isEmpty, err := getMinMaxValues(ctx, primaryKey, w.config.fetchWorkerConfig)
		if err != nil {
			return false, fmt.Errorf("failed to get min max values: %w", err)
		} else if isEmpty {
			return true, nil
		}

		w.start = append(w.start, common.TablePositionMultipleKeyItem{
			KeyName:     primaryKey,
			LastRead:    scanned.MinValue,
			SnapshotEnd: scanned.MaxValue,
		})
		w.end = append(w.end, common.TablePositionMultipleKeyItem{
			KeyName:     primaryKey,
			LastRead:    scanned.MinValue,
			SnapshotEnd: scanned.MaxValue,
		})
	}

	if last, ok := w.usableLastPosition(ctx); ok {
		w.start = last
		w.resume = true
	}

	sdk.Logger(ctx).Debug().
		Any("start", w.start).
		Type("start type", w.start).
		Any("end", w.end).
		Type("end type", w.end).
		Msg("fetched start and end")

	return false, nil
}

// usableLastPosition returns the persisted position with its key names filled in,
// or false when there is none or it does not describe this table's current key
// (a different number of columns, or different column names). An unusable
// position restarts the table from its first row: duplicates, never a gap.
//
// Positions written before key names were recorded carry an empty key_name; they
// are matched to the key columns by index.
func (w *fetchWorkerMultipleKey) usableLastPosition(ctx context.Context) (common.TablePositionMultipleKey, bool) {
	last := w.config.lastPosition
	if len(last) == 0 {
		return nil, false
	}

	keys := w.config.primaryKeys
	if len(last) != len(keys) {
		sdk.Logger(ctx).Warn().Str("table", w.config.table).
			Msg("persisted composite key position does not match the table's key columns, restarting the table snapshot")
		return nil, false
	}

	resolved := make(common.TablePositionMultipleKey, len(last))
	for i, item := range last {
		if item.KeyName != "" && item.KeyName != keys[i] {
			sdk.Logger(ctx).Warn().Str("table", w.config.table).
				Msg("persisted composite key position does not match the table's key columns, restarting the table snapshot")
			return nil, false
		}
		item.KeyName = keys[i]
		resolved[i] = item
	}

	return resolved, true
}

func (w *fetchWorkerMultipleKey) run(ctx context.Context) error {
	sdk.Logger(ctx).Debug().Msgf("started fetch worker by keys for table %q", w.config.table)
	defer sdk.Logger(ctx).Debug().Msgf("finished fetch worker by keys for table %q", w.config.table)

	tx, err := w.config.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}
	defer func() { err = errors.Join(err, tx.Commit()) }()

	sdk.Logger(ctx).Debug().Msgf("obtained tx for table %v", w.config.table)
	sdk.Logger(ctx).Info().
		Any("start", w.start).
		Any("end", w.end).
		Uint64("fetchSize", w.config.fetchSize).
		Msg("fetching rows")

	// If the worker has been given a starting position it means that we have already
	// read the record in that specific position, so we can just exclude it.

	discardFirst := w.resume
	chunkStart := w.start
	for {
		sdk.Logger(ctx).Info().
			Any("chunk start", chunkStart).
			Any("end", w.end).Msg("fetching chunk")

		rowsChunk, err := w.selectRowsChunk(ctx, tx, chunkStart, discardFirst)
		if err != nil {
			return fmt.Errorf("failed to select rows chunk: %w", err)
		}
		discardFirst = true

		for _, row := range rowsChunk.rows {
			sdk.Logger(ctx).Trace().Msgf("fetched row: %+v", row)

			lastRead, found := w.getKeyPos(row)
			if !found {
				return ErrPrimaryKeyNotFoundInRow
			}

			data, err := w.buildFetchData(ctx, row, rowsChunk.mysqlAvroCols, lastRead)
			if err != nil {
				return fmt.Errorf("failed to build fetch data: %w", err)
			}

			select {
			case w.config.data <- data:
			case <-ctx.Done():
				return fmt.Errorf(
					"fetch worker context done while waiting for data: %w", ctx.Err(),
				)
			}

			chunkStart = lastRead
		}

		if rowsChunk.foundEnd {
			break
		}
	}

	return nil
}

func (w *fetchWorkerMultipleKey) getKeyPos(
	row map[string]any,
) (pos common.TablePositionMultipleKey, found bool) {
	for _, key := range w.config.primaryKeys {
		val, ok := row[key]
		if !ok {
			return nil, false
		}

		pos = append(pos, common.TablePositionMultipleKeyItem{KeyName: key, LastRead: val})
	}

	return pos, true
}

func (w *fetchWorkerMultipleKey) selectRowsChunk(
	ctx context.Context, tx *sqlx.Tx,
	start common.TablePositionMultipleKey, discardFirst bool,
) (*rowsChunk, error) {
	stmt := squirrel.Select("*").From(w.config.table)

	// Seek past the last delivered row. The predicate must be lexicographic over
	// the key columns; independent "col > ?" terms ANDed together skip every row
	// that is larger in one column and not larger in another (issue #189).
	// The first chunk has nothing to seek past: start is only the table minimum.
	if discardFirst {
		// Invariant 3: bind keys typed; see typeSettings.bindKey.
		bound := make(common.TablePositionMultipleKey, len(start))
		copy(bound, start)
		for i := range bound {
			bound[i].LastRead = w.config.types.bindKey(w.config.table, bound[i].KeyName, bound[i].LastRead)
		}
		stmt = stmt.Where(lexicographicAfter(bound))
	}

	query, args, err := stmt.
		OrderBy(w.config.primaryKeys...).
		Limit(w.config.fetchSize).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	sdk.Logger(ctx).Debug().Str("query", query).Any("args", args).Msg("created query")

	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			closeErr = fmt.Errorf("failed to close rows: %w", closeErr)
			err = errors.Join(err, closeErr)
		}
	}()

	chunk := &rowsChunk{}

	chunk.mysqlAvroCols, err = sqlxRowsToAvroCol(rows)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column types: %w", err)
	}

	for rows.Next() {
		row := map[string]any{}
		if err := rows.MapScan(row); err != nil {
			return nil, fmt.Errorf("failed to map scan row: %w", err)
		}

		chunk.rows = append(chunk.rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to close rows: %w", err)
	}

	//nolint:gosec // fetchSize is already checked for being a sane int value
	chunk.foundEnd = len(chunk.rows) < int(w.config.fetchSize)

	return chunk, nil
}

// lexicographicAfter builds the predicate for "rows whose key tuple sorts after
// last", i.e. (k1 > v1) OR (k1 = v1 AND k2 > v2) OR ... It is spelled out
// rather than as the row constructor (k1, k2) > (v1, v2) so that MySQL can
// use the primary key index for the range on every supported version.
func lexicographicAfter(last common.TablePositionMultipleKey) squirrel.Sqlizer {
	after := make(squirrel.Or, 0, len(last))
	for i := range last {
		term := make(squirrel.And, 0, i+1)
		for _, prev := range last[:i] {
			term = append(term, squirrel.Eq{prev.KeyName: prev.LastRead})
		}
		term = append(term, squirrel.Gt{last[i].KeyName: last[i].LastRead})
		after = append(after, term)
	}

	return after
}

func (w *fetchWorkerMultipleKey) buildFetchData(
	ctx context.Context, row map[string]any,
	colTypes []*avroNamedType, lastRead common.TablePositionMultipleKey,
) (fetchData, error) {
	// Record each key column's type next to its value; see
	// common.TablePositionSingleKey.KeyType. lastRead is built by getKeyPos in
	// the order of the primary keys.
	typed := make(common.TablePositionMultipleKey, len(lastRead))
	copy(typed, lastRead)
	for i := range typed {
		if i >= len(w.config.primaryKeys) {
			break
		}
		for _, colType := range colTypes {
			if colType.Name == w.config.primaryKeys[i] {
				typed[i].KeyType = colType.dbType
				break
			}
		}
	}
	position := common.TablePosition{
		MultipleKey: &typed,
	}

	payloadSubver, err := w.payloadSchema.createPayloadSchema(ctx, w.config.table, colTypes)
	if err != nil {
		return fetchData{}, fmt.Errorf("failed to create payload schema for table %s: %w", w.config.table, err)
	}

	payload := make(opencdc.StructuredData)
	for key, val := range row {
		formatted, err := w.payloadSchema.formatValue(ctx, key, val)
		if err != nil {
			return fetchData{}, err
		}
		payload[key] = formatted
	}

	var keyColTypes []*avroNamedType
keyColLoop:
	for _, primaryKey := range w.config.primaryKeys {
		for _, colType := range colTypes {
			if primaryKey == colType.Name {
				keyColTypes = append(keyColTypes, colType)
				continue keyColLoop
			}
		}
		return fetchData{}, fmt.Errorf("failed to find key schema column type for table %s", w.config.table)
	}

	keySubver, err := w.keySchema.createKeySchema(ctx, w.config.table, keyColTypes)
	if err != nil {
		return fetchData{}, fmt.Errorf("failed to create key schema for table %s: %w", w.config.table, err)
	}

	key := opencdc.StructuredData{}
	for _, primaryKey := range w.config.primaryKeys {
		keyVal, ok := row[primaryKey]
		if !ok {
			return fetchData{}, fmt.Errorf("key %s not found in payload", primaryKey)
		}
		formatted, err := w.keySchema.formatValue(ctx, primaryKey, keyVal)
		if err != nil {
			return fetchData{}, err
		}
		key[primaryKey] = formatted
	}

	return fetchData{
		key:           key,
		table:         w.config.table,
		payload:       payload,
		position:      position,
		payloadSchema: payloadSubver,
		keySchema:     keySubver,
	}, nil
}

type minmaxRow struct {
	MinValue any `db:"min_value"`
	MaxValue any `db:"max_value"`
}

// getMinMaxValues fetches the maximum value of the primary key from the table.
func getMinMaxValues(
	ctx context.Context, primaryKey string, config fetchWorkerConfig,
) (scanned *minmaxRow, isTableEmpty bool, err error) {
	var scannedRow minmaxRow

	query := fmt.Sprintf(
		"SELECT MIN(%s) as min_value, MAX(%s) as max_value FROM %s",
		primaryKey, primaryKey, config.table)

	row := config.db.QueryRowxContext(ctx, query)
	if err := row.StructScan(&scannedRow); err != nil {
		return nil, false, fmt.Errorf("failed to get min value: %w", err)
	}
	if err := row.Err(); err != nil {
		return nil, false, fmt.Errorf("failed to get min value: %w", err)
	}

	if scannedRow.MinValue == nil || scannedRow.MaxValue == nil {
		return nil, true, nil
	}

	sdk.Logger(ctx).Debug().
		Str("query", query).
		Str("min_value_type", fmt.Sprintf("%T", scannedRow.MinValue)).
		Str("max_value_type", fmt.Sprintf("%T", scannedRow.MaxValue)).
		Any("scannedRow", scannedRow).
		Send()

	return &scannedRow, false, nil
}
