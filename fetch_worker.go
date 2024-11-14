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

var ErrSortingKeyNotFoundInRow = errors.New("sorting key not found in row")

type fetchWorker struct {
	db         *sqlx.DB
	data       chan fetchData
	config     fetchWorkerConfig
	start, end any
}

type fetchWorkerConfig struct {
	lastPosition common.SnapshotPosition
	table        string
	fetchSize    uint64
	sortColName  string
}

func newFetchWorker(db *sqlx.DB, data chan fetchData, config fetchWorkerConfig) *fetchWorker {
	return &fetchWorker{
		db:     db,
		data:   data,
		config: config,
	}
}

func (w *fetchWorker) fetchStartEnd(ctx context.Context) (isTableEmpty bool, err error) {
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

	sdk.Logger(ctx).Info().
		Any("start", w.start).
		Any("end", w.end).
		Msg("fetched start and end")

	return false, nil
}

func (w *fetchWorker) run(ctx context.Context) (err error) {
	sdk.Logger(ctx).Info().Msgf("started fetcher for table %q", w.config.table)
	defer sdk.Logger(ctx).Info().Msgf("finished fetcher for table %q", w.config.table)

	tx, err := w.db.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	sdk.Logger(ctx).Info().Msgf("obtained tx for table %v", w.config.table)

	defer func() { err = errors.Join(err, tx.Commit()) }()

	sdk.Logger(ctx).Info().
		Any("start", w.start).
		Any("end", w.end).
		Uint64("fetchSize", w.config.fetchSize).
		Msg("fetching rows")

	// We need to batch the iteration in chunks because mysql cursors are very
	// slow, compared to postgres.

	// If the worker has been given a starting position it means that we have already
	// read the record in that specific position, so we can just exclude it.

	discardFirst := w.config.lastPosition.Snapshots[w.config.table].LastRead != nil
	chunkStart := w.start
	for {
		greaterOrEq, cantCompare := common.IsGreaterOrEqual(chunkStart, w.end)
		if cantCompare {
			return fmt.Errorf(
				"cannot compare values %v and %v of types %T and %T",
				chunkStart, w.end, chunkStart, w.end)
		} else if greaterOrEq {
			break
		}

		sdk.Logger(ctx).Info().
			Any("chunk start", chunkStart).
			Any("end", w.end).
			Msg("fetching chunk")

		rows, err := w.selectRowsChunk(ctx, tx, chunkStart, discardFirst)
		if err != nil {
			return fmt.Errorf("failed to select rows chunk: %w", err)
		}
		discardFirst = true

		if len(rows) == 0 {
			continue
		}

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
	}

	return nil
}

type minmaxRow struct {
	MinValue any `db:"min_value"`
	MaxValue any `db:"max_value"`
}

// getMinMaxValues fetches the maximum value of the primary key from the table.
func (w *fetchWorker) getMinMaxValues(
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

func (w *fetchWorker) selectRowsChunk(
	ctx context.Context, tx *sqlx.Tx,
	start any, discardFirst bool,
) (scannedRows []opencdc.StructuredData, err error) {
	var wherePred any = squirrel.GtOrEq{w.config.sortColName: start}
	if discardFirst {
		wherePred = squirrel.Gt{w.config.sortColName: start}
	}

	query, args, err := squirrel.
		Select("*").
		From(w.config.table).
		Where(wherePred).
		OrderBy(w.config.sortColName).
		Limit(w.config.fetchSize).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	sdk.Logger(ctx).Trace().Str("query", query).Any("args", args).Msg("created query")

	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows: %w", err)
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
			return nil, fmt.Errorf("failed to map scan row: %w", err)
		}

		// convert the values so that they can be easily serialized
		for key, val := range row {
			row[key] = common.FormatValue(val)
		}

		scannedRows = append(scannedRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to close rows: %w", err)
	}

	return scannedRows, nil
}

func (w *fetchWorker) buildFetchData(
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
