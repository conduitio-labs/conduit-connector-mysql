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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Masterminds/squirrel"
	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

var ErrSortingKeyNotFoundInRow = errors.New("sorting key not found in row")

type fetchWorker interface {
	// for debug purposes
	table() string

	run(ctx context.Context) error
	fetchStartEnd(ctx context.Context) (tableEmpty bool, err error)
}

type fetchWorkerConfig struct {
	lastPosition common.SnapshotPosition
	table        string
	fetchSize    uint64
	sortColName  string
}

func newFetchWorker(db *sqlx.DB, data chan fetchData, config fetchWorkerConfig) fetchWorker {
	if config.sortColName == "" {
		return newFetchWorkerByLimit(db, data, config)
	}

	return newFetchWorkerByKey(db, data, config)
}

// fetchWorkerByKey will perform a snapshot using the sortColName as
// the sorting key for fetching rows in chunks.
type fetchWorkerByKey struct {
	db         *sqlx.DB
	data       chan fetchData
	config     fetchWorkerConfig
	start, end any
}

func newFetchWorkerByKey(db *sqlx.DB, data chan fetchData, config fetchWorkerConfig) fetchWorker {
	return &fetchWorkerByKey{
		db:     db,
		data:   data,
		config: config,
	}
}

func (w *fetchWorkerByKey) table() string {
	return w.config.table
}

func (w *fetchWorkerByKey) fetchStartEnd(ctx context.Context) (isTableEmpty bool, err error) {
	row, isEmpty, err := w.getMinMaxValues(ctx)
	if err != nil {
		return false, err
	} else if isEmpty {
		return true, nil
	}

	w.start = row.MinValue
	lastRead := w.config.lastPosition.Snapshots[w.config.table].LastRead
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

func (w *fetchWorkerByKey) run(ctx context.Context) (err error) {
	sdk.Logger(ctx).Info().Msgf("started fetch worker by key for table %q", w.config.table)
	defer sdk.Logger(ctx).Info().Msgf("finished fetch worker by key for table %q", w.config.table)

	tx, err := w.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	sdk.Logger(ctx).Debug().Msgf("obtained tx for table %v", w.config.table)

	defer func() { err = errors.Join(err, tx.Commit()) }()

	sdk.Logger(ctx).Info().
		Any("start", w.start).
		Any("end", w.end).
		Uint64("fetchSize", w.config.fetchSize).
		Msg("fetching rows")

	// If the worker has been given a starting position it means that we have already
	// read the record in that specific position, so we can just exclude it.

	discardFirst := w.config.lastPosition.Snapshots[w.config.table].LastRead != nil
	chunkStart := w.start
	for {
		sdk.Logger(ctx).Info().
			Any("chunk start", chunkStart).
			Any("end", w.end).Msg("fetching chunk")

		rows, foundEnd, err := w.selectRowsChunk(ctx, tx, chunkStart, discardFirst)
		if err != nil {
			return fmt.Errorf("failed to select rows chunk: %w", err)
		}
		discardFirst = true

		for _, row := range rows {
			sdk.Logger(ctx).Trace().Msgf("fetched row: %+v", row)

			lastRead := row[w.config.sortColName]
			if lastRead == nil {
				return ErrSortingKeyNotFoundInRow
			}

			position := common.TablePosition{
				LastRead:    lastRead,
				SnapshotEnd: w.end,
			}
			data, err := w.buildFetchData(row, position)
			if err != nil {
				return fmt.Errorf("failed to build fetch data: %w", err)
			}

			select {
			case w.data <- data:
			case <-ctx.Done():
				return fmt.Errorf(
					"fetch worker context done while waiting for data: %w", ctx.Err(),
				)
			}

			chunkStart = lastRead
		}

		if foundEnd {
			break
		}
	}

	return nil
}

type minmaxRow struct {
	MinValue any `db:"min_value"`
	MaxValue any `db:"max_value"`
}

// getMinMaxValues fetches the maximum value of the primary key from the table.
func (w *fetchWorkerByKey) getMinMaxValues(
	ctx context.Context,
) (scanned *minmaxRow, isTableEmpty bool, err error) {
	var scannedRow minmaxRow

	query := fmt.Sprintf(
		"SELECT MIN(%s) as min_value, MAX(%s) as max_value FROM %s",
		w.config.sortColName, w.config.sortColName, w.config.table)

	row := w.db.QueryRowxContext(ctx, query)
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

func (w *fetchWorkerByKey) selectRowsChunk(
	ctx context.Context, tx *sqlx.Tx,
	start any, discardFirst bool,
) (scannedRows []opencdc.StructuredData, foundEnd bool, err error) {
	var wherePred any = squirrel.GtOrEq{w.config.sortColName: start}
	if discardFirst {
		wherePred = squirrel.Gt{w.config.sortColName: start}
	}

	query, args, err := squirrel.
		Select("*").
		From(w.config.table).
		Where(wherePred).
		Where(squirrel.LtOrEq{w.config.sortColName: w.end}).
		OrderBy(w.config.sortColName).
		Limit(w.config.fetchSize).
		ToSql()
	if err != nil {
		return nil, false, fmt.Errorf("failed to build query: %w", err)
	}

	sdk.Logger(ctx).Debug().Str("query", query).Any("args", args).Msg("created query")

	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("failed to query rows: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err != nil {
			if err == nil {
				err = fmt.Errorf("failed to close rows: %w", closeErr)
			} else {
				err = errors.Join(err, fmt.Errorf("failed to close rows: %w", closeErr))
			}
		}
	}()

	for rows.Next() {
		row := opencdc.StructuredData{}
		if err := rows.MapScan(row); err != nil {
			return nil, false, fmt.Errorf("failed to map scan row: %w", err)
		}

		// convert the values so that they can be easily serialized
		for key, val := range row {
			row[key] = common.FormatValue(val)
		}

		scannedRows = append(scannedRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("failed to close rows: %w", err)
	}

	//nolint:gosec // fetchSize is already checked for being a sane int value
	foundEnd = len(scannedRows) < int(w.config.fetchSize)

	return scannedRows, foundEnd, nil
}

func (w *fetchWorkerByKey) buildFetchData(
	payload opencdc.StructuredData,
	position common.TablePosition,
) (fetchData, error) {
	keyVal, ok := payload[w.config.sortColName]
	if !ok {
		return fetchData{}, fmt.Errorf("key %s not found in payload", w.config.sortColName)
	}

	key := snapshotKey{w.config.sortColName, keyVal}
	return fetchData{key, w.config.table, payload, position}, nil
}

// fetchWorkerByLimit will perform a snapshot using the LIMIT + OFFSET clauses to fetch
// rows in chunks.
type fetchWorkerByLimit struct {
	config fetchWorkerConfig
	db     *sqlx.DB
	data   chan fetchData
	end    uint64
}

func newFetchWorkerByLimit(
	db *sqlx.DB, data chan fetchData, config fetchWorkerConfig,
) fetchWorker {
	return &fetchWorkerByLimit{
		db:     db,
		data:   data,
		config: config,
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

	row := w.db.QueryRowxContext(ctx, query, args...)
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
	sdk.Logger(ctx).Info().Msgf("started fetch worker by limit for table %q", w.config.table)
	defer sdk.Logger(ctx).Info().Msgf("finished fetch worker by limit for table %q", w.config.table)

	tx, err := w.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}
	defer func() {
		if err = tx.Commit(); err != nil {
			err = fmt.Errorf("failed to commit transaction: %w", err)
		}
	}()

	sdk.Logger(ctx).Debug().Msgf("obtained tx for table %v", w.config.table)

	for offset := uint64(0); offset < w.end; offset += w.config.fetchSize {
		query, args, err := squirrel.
			Select("*").From(w.config.table).
			Limit(w.config.fetchSize).Offset(offset).ToSql()
		if err != nil {
			return fmt.Errorf("failed to build query: %w", err)
		}

		sdk.Logger(ctx).Debug().Str("query", query).Any("args", args).Msg("created query")

		rows, err := tx.QueryxContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to query rows: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			row := opencdc.StructuredData{}
			if err := rows.MapScan(row); err != nil {
				return fmt.Errorf("failed to map scan row: %w", err)
			}

			// convert the values so that they can be easily serialized
			for key, val := range row {
				row[key] = common.FormatValue(val)
			}

			keyBytes, err := json.Marshal(row)
			if err != nil {
				return fmt.Errorf("failed to marshal row: %w", err)
			}
			encodedKey := base64.StdEncoding.EncodeToString(keyBytes)

			// we don't really have any way to get the key from the table, so we
			// make up for one
			key := snapshotKey{
				Key:   "",
				Value: encodedKey,
			}

			w.data <- fetchData{
				key:     key,
				table:   w.config.table,
				payload: row,

				// ignoring position, we start from 0 each time
				// TODO: we could save the offset as a position, we might want
				// to do it in the future.
				position: common.TablePosition{},
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("failed to close rows: %w", err)
		}
	}

	return nil
}
