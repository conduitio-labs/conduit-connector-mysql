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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

type fetchWorker struct {
	db     *sqlx.DB
	data   chan fetchData
	config fetchWorkerConfig
}

type fetchWorkerConfig struct {
	lastPosition common.SnapshotPosition
	table        common.TableName
	fetchSize    int
	primaryKey   common.PrimaryKeyName
}

func newFetchWorker(db *sqlx.DB, data chan fetchData, config fetchWorkerConfig) *fetchWorker {
	return &fetchWorker{
		db:     db,
		data:   data,
		config: config,
	}
}

func (w *fetchWorker) run(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msgf("starting fetcher for table %q", w.config.table)

	lastRead := w.config.lastPosition.Snapshots[w.config.table].LastRead
	for {
		snapshotEnd, err := w.getMaxValue(ctx)
		if err != nil {
			return fmt.Errorf("failed to get max value: %w", err)
		}

		rows, err := w.selectRowsChunk(ctx, lastRead, snapshotEnd)
		if err != nil {
			return fmt.Errorf("failed to select rows chunk: %w", err)
		}
		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			lastRead++
			position := common.TablePosition{
				LastRead:    lastRead,
				SnapshotEnd: snapshotEnd,
			}
			data, err := w.buildFetchData(row, position)
			if err != nil {
				return fmt.Errorf("failed to build fetch data: %w", err)
			}

			w.data <- data
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
	ctx context.Context,
	start, end int64,
) (scannedRows []sdk.StructuredData, err error) {
	query := fmt.Sprint(`
		SELECT *
		FROM `, w.config.table, `
		WHERE `, w.config.primaryKey, ` > ? AND `, w.config.primaryKey, ` <= ?
		ORDER BY `, w.config.primaryKey, ` LIMIT ?
	`)
	logDataEvt := sdk.Logger(ctx).Debug().
		Any("data", sdk.StructuredData{
			"query":     query,
			"start":     start,
			"end":       end,
			"fetchSize": w.config.fetchSize,
		})

	rows, err := w.db.QueryxContext(ctx, query, start, end, w.config.fetchSize)
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
		row := sdk.StructuredData{}
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
	payload sdk.StructuredData,
	position common.TablePosition,
) (fetchData, error) {
	keyVal, ok := payload[string(w.config.primaryKey)]
	if !ok {
		return fetchData{}, fmt.Errorf("key %s not found in payload", w.config.primaryKey)
	}

	return fetchData{
		key: snapshotKey{
			Table: w.config.table,
			Key:   w.config.primaryKey,
			Value: keyVal,
		},
		payload:  payload,
		position: position,
	}, nil
}
