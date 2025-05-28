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
}

func newFetchWorker(ctx context.Context, config fetchWorkerConfig) fetchWorker {
	switch len(config.primaryKeys) {
	case 0:
		return newFetchWorkerByLimit(config)
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
			sdk.Logger(ctx).Warn().
				Str("table", config.table).
				Msg("multiple key position not found in last position, defaulting to fetch worker by limit iteration")

			return newFetchWorkerByLimit(config)
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
		payloadSchema: newSchemaMapper(),
		keySchema:     newSchemaMapper(),
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
	var whereStart any = squirrel.GtOrEq{w.primaryKey: start}
	if discardFirst {
		whereStart = squirrel.Gt{w.primaryKey: start}
	}
	whereEnd := squirrel.LtOrEq{w.primaryKey: w.end}

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
	position := common.TablePosition{
		SingleKey: &common.TablePositionSingleKey{
			LastRead:    lastRead,
			SnapshotEnd: w.end,
		},
	}

	payloadSubver, err := w.payloadSchema.createPayloadSchema(ctx, w.config.table, colTypes)
	if err != nil {
		return fetchData{}, fmt.Errorf("failed to create payload schema for table %s: %w", w.config.table, err)
	}

	payload := make(opencdc.StructuredData)
	for key, val := range row {
		payload[key] = w.payloadSchema.formatValue(ctx, key, val)
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
	keyVal = w.keySchema.formatValue(ctx, w.primaryKey, keyVal)

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
}

type fetchWorkerByKeysConfig struct {
	fetchWorkerConfig

	lastPosition common.TablePositionMultipleKey
}

func newFetchWorkerByKeys(config fetchWorkerByKeysConfig) fetchWorker {
	return &fetchWorkerMultipleKey{
		config:        config,
		payloadSchema: newSchemaMapper(),
		keySchema:     newSchemaMapper(),
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

	if len(w.config.primaryKeys) == len(w.config.lastPosition) {
		w.start = w.config.lastPosition
	}

	sdk.Logger(ctx).Debug().
		Any("start", w.start).
		Type("start type", w.start).
		Any("end", w.end).
		Type("end type", w.end).
		Msg("fetched start and end")

	return false, nil
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

	discardFirst := len(w.config.lastPosition) > 0
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

		pos = append(pos, common.TablePositionMultipleKeyItem{LastRead: val})
	}

	return pos, true
}

func (w *fetchWorkerMultipleKey) selectRowsChunk(
	ctx context.Context, tx *sqlx.Tx,
	start common.TablePositionMultipleKey, discardFirst bool,
) (*rowsChunk, error) {
	stmt := squirrel.Select("*").From(w.config.table)

	// build where
	for _, keyPos := range start {
		if discardFirst {
			stmt = stmt.Where(squirrel.Gt{keyPos.KeyName: keyPos.LastRead})
		} else {
			stmt = stmt.Where(squirrel.GtOrEq{keyPos.KeyName: keyPos.LastRead})
		}
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

func (w *fetchWorkerMultipleKey) buildFetchData(
	ctx context.Context, row map[string]any,
	colTypes []*avroNamedType, lastRead common.TablePositionMultipleKey,
) (fetchData, error) {
	position := common.TablePosition{
		MultipleKey: &lastRead,
	}

	payloadSubver, err := w.payloadSchema.createPayloadSchema(ctx, w.config.table, colTypes)
	if err != nil {
		return fetchData{}, fmt.Errorf("failed to create payload schema for table %s: %w", w.config.table, err)
	}

	payload := make(opencdc.StructuredData)
	for key, val := range row {
		payload[key] = w.payloadSchema.formatValue(ctx, key, val)
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
		key[primaryKey] = w.keySchema.formatValue(ctx, primaryKey, keyVal)
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

// fetchWorkerByLimit will perform a snapshot using the LIMIT + OFFSET clauses to fetch
// rows in chunks.
type fetchWorkerByLimit struct {
	config fetchWorkerConfig
	end    uint64

	payloadSchema *schemaMapper
}

func newFetchWorkerByLimit(config fetchWorkerConfig) fetchWorker {
	return &fetchWorkerByLimit{
		payloadSchema: newSchemaMapper(),
		config:        config,
	}
}

func (w *fetchWorkerByLimit) table() string {
	return w.config.table
}

func (w *fetchWorkerByLimit) countTotal(ctx context.Context) (uint64, error) {
	var total struct {
		Total uint64 `db:"total"`
	}

	query, args, err := squirrel.Select("COUNT(*) as total").From(w.config.table).ToSql()
	if err != nil {
		return 0, fmt.Errorf("failed to build query: %w", err)
	}

	row := w.config.db.QueryRowxContext(ctx, query, args...)
	if err := row.StructScan(&total); err != nil {
		return 0, fmt.Errorf("failed to fetch total from %s: %w", w.config.table, err)
	} else if err := row.Err(); err != nil {
		return 0, fmt.Errorf("failed to fetch total from %s: %w", w.config.table, err)
	}

	sdk.Logger(ctx).Debug().
		Str("query", query).Any("args", args).
		Uint64("result", total.Total).
		Msg("count query")

	return total.Total, nil
}

func (w *fetchWorkerByLimit) fetchStartEnd(ctx context.Context) (isTableEmpty bool, err error) {
	total, err := w.countTotal(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get total: %w", err)
	} else if total == 0 {
		return true, nil
	}

	w.end = total

	return false, nil
}

func (w *fetchWorkerByLimit) run(ctx context.Context) (err error) {
	logger := sdk.Logger(ctx).With().Str("table", w.config.table).Logger()

	logger.Debug().Msgf("started fetch worker by limit")
	defer logger.Debug().Msgf("finished fetch worker by limit")

	for offset := uint64(0); offset < w.end; offset += w.config.fetchSize {
		query, args, err := squirrel.
			Select("*").From(w.config.table).
			Limit(w.config.fetchSize).Offset(offset).ToSql()
		if err != nil {
			return fmt.Errorf("failed to build query: %w", err)
		}

		logger.Debug().Str("query", query).Any("args", args).Msg("created query")

		sqlxRows, err := w.config.db.QueryxContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query rows: %w", err)
		}
		defer func() {
			if closeErr := sqlxRows.Close(); closeErr != nil {
				closeErr = fmt.Errorf("failed to close rows: %w", closeErr)
				err = errors.Join(err, closeErr)
			}
		}()
		colTypes, err := sqlxRowsToAvroCol(sqlxRows)
		if err != nil {
			return fmt.Errorf("failed to retrieve column types: %w", err)
		}

		rows := []map[string]any{}
		for sqlxRows.Next() {
			row := map[string]any{}
			if err := sqlxRows.MapScan(row); err != nil {
				return fmt.Errorf("failed to map scan row: %w", err)
			}
			rows = append(rows, row)
		}
		if err := sqlxRows.Err(); err != nil {
			return fmt.Errorf("failed to close rows: %w", err)
		}

		for i, row := range rows {
			logger.Trace().Msgf("fetched row: %+v", row)

			payloadSubver, err := w.payloadSchema.createPayloadSchema(ctx, w.config.table, colTypes)
			if err != nil {
				return fmt.Errorf("failed to create payload schema for table %s: %w", w.config.table, err)
			}

			payload := make(opencdc.StructuredData)
			for key, val := range row {
				payload[key] = w.payloadSchema.formatValue(ctx, key, val)
			}

			//nolint:gosec // i is guaranteed to be greater than 0
			rowNum := offset + uint64(i)
			keyStr := fmt.Sprintf("%s_%d", w.config.table, rowNum)
			key := opencdc.RawData(keyStr)

			w.config.data <- fetchData{
				key:           key,
				table:         w.config.table,
				payload:       payload,
				payloadSchema: payloadSubver,

				// ignoring position, we start from 0 each time
				// TODO: we could save the offset as a position, we might want
				// to do it in the future.
				position: common.TablePosition{},
			}
		}
	}

	return nil
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
