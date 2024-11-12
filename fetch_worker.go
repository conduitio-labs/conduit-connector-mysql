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

func (w *fetchWorker) fetchStartEnd(ctx context.Context) (err error) {
	minVal, maxVal, err := w.getMinMaxValues(ctx)
	if err != nil {
		return err
	}

	lastRead := w.config.lastPosition.Snapshots[w.config.table].LastRead
	if lastRead != nil {
		w.start = lastRead
	} else {
		w.start = minVal
	}
	w.end = maxVal

	sdk.Logger(ctx).Info().
		Any("start", w.start).
		Any("end", w.end).
		Msg("fetched start and end")

	return nil
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

	chunkStart := w.start
	for {
		equal, cantCompare := common.AreEqual(chunkStart, w.end)
		if cantCompare {
			return fmt.Errorf("cannot compare values %v and %v", chunkStart, w.end)
		} else if equal {
			break
		}

		sdk.Logger(ctx).Info().
			Any("chunk start", chunkStart).
			Any("end", w.end).
			Msg("fetching chunk")

		rows, err := w.selectRowsChunk(ctx, tx, chunkStart)
		if err != nil {
			return fmt.Errorf("failed to select rows chunk: %w", err)
		}
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

// getMinMaxValues fetches the maximum value of the primary key from the table.
func (w *fetchWorker) getMinMaxValues(ctx context.Context) (minVal, maxVal any, err error) {
	var minmax struct {
		MinValue any `db:"min_value"`
		MaxValue any `db:"max_value"`
	}

	query := fmt.Sprintf(
		"SELECT MIN(%s) as min_value, MAX(%s) as max_value FROM %s",
		w.config.sortColName, w.config.sortColName, w.config.table,
	)
	row := w.db.QueryRowxContext(ctx, query)
	if err := row.StructScan(&minmax); err != nil {
		return nil, nil, fmt.Errorf("failed to get min value: %w", err)
	}

	if err := row.Err(); err != nil {
		return nil, nil, fmt.Errorf("failed to get min value: %w", err)
	}

	return minmax.MinValue, minmax.MaxValue, nil
}

func (w *fetchWorker) selectRowsChunk(
	ctx context.Context, tx *sqlx.Tx,
	start any,
) (scannedRows []opencdc.StructuredData, err error) {

	var wherePred any = squirrel.GtOrEq{w.config.sortColName: start}
	startingFromPos := w.config.lastPosition.Snapshots[w.config.table].LastRead != nil
	if startingFromPos {
		// If the worker has been given a starting position it means that we have already
		// read the record in that specific position, so we can just exclude it.
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

	return fetchData{
		key: snapshotKey{
			Key:   w.config.sortColName,
			Value: keyVal,
		},
		table:    w.config.table,
		payload:  payload,
		position: position,
	}, nil
}
