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

type fetchWorker struct {
	db         *sqlx.DB
	data       chan fetchData
	config     fetchWorkerConfig
	start, end int64
}

type fetchWorkerConfig struct {
	lastPosition common.SnapshotPosition
	table        string
	fetchSize    uint64
	primaryKey   string
}

func newFetchWorker(db *sqlx.DB, data chan fetchData, config fetchWorkerConfig) *fetchWorker {
	return &fetchWorker{
		db:     db,
		data:   data,
		config: config,
	}
}

func (w *fetchWorker) fetchStartEnd(ctx context.Context) (err error) {
	w.start = w.config.lastPosition.Snapshots[w.config.table].LastRead
	w.end, err = w.getMaxValue(ctx)
	return err
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

	start, end := w.start, w.end
	for {
		rows, err := w.selectRowsChunk(ctx, tx, start, end)
		if err != nil {
			return fmt.Errorf("failed to select rows chunk: %w", err)
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			start++
			position := common.TablePosition{
				LastRead:    start,
				SnapshotEnd: end,
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
		}
	}

	return nil
}

// getMaxValue fetches the maximum value of the primary key from the table.
func (w *fetchWorker) getMaxValue(ctx context.Context) (int64, error) {
	var maxValueRow struct {
		MaxValue *int64 `db:"max_value"`
	}

	query := fmt.Sprintf(
		"SELECT MAX(%s) as max_value FROM %s",
		w.config.primaryKey, w.config.table,
	)
	row := w.db.QueryRowxContext(ctx, query)
	if err := row.StructScan(&maxValueRow); err != nil {
		return 0, fmt.Errorf("failed to get max value: %w", err)
	}

	if err := row.Err(); err != nil {
		return 0, fmt.Errorf("failed to get max value: %w", err)
	}

	if maxValueRow.MaxValue == nil {
		// table is empty
		return 0, nil
	}

	return *maxValueRow.MaxValue, nil
}

func (w *fetchWorker) selectRowsChunk(
	ctx context.Context, tx *sqlx.Tx,
	start, end int64,
) (scannedRows []opencdc.StructuredData, err error) {
	query, args, err := squirrel.
		Select("*").
		From(w.config.table).
		Where(squirrel.Gt{w.config.primaryKey: start}).
		Where(squirrel.LtOrEq{w.config.primaryKey: end}).
		OrderBy(w.config.primaryKey).
		Limit(w.config.fetchSize).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	logDataEvt := sdk.Logger(ctx).Debug().
		Any("data", opencdc.StructuredData{
			"query":     query,
			"start":     start,
			"end":       end,
			"fetchSize": w.config.fetchSize,
		})

	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		logDataEvt.Msg("failed to query rows")
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
			logDataEvt.Msg("failed to map scan row")
			return nil, fmt.Errorf("failed to map scan row: %w", err)
		}

		// convert the values so that they can be easily serialized
		for key, val := range row {
			row[key] = common.FormatValue(val)
		}

		scannedRows = append(scannedRows, row)
	}
	if err := rows.Err(); err != nil {
		logDataEvt.Msg("error occurred during row iteration")
		return nil, fmt.Errorf("failed to close rows: %w", err)
	}

	return scannedRows, nil
}

func (w *fetchWorker) buildFetchData(
	payload opencdc.StructuredData,
	position common.TablePosition,
) (fetchData, error) {
	keyVal, ok := payload[w.config.primaryKey]
	if !ok {
		return fetchData{}, fmt.Errorf("key %s not found in payload", w.config.primaryKey)
	}

	return fetchData{
		key: snapshotKey{
			Key:   w.config.primaryKey,
			Value: keyVal,
		},
		table:    w.config.table,
		payload:  payload,
		position: position,
	}, nil
}
