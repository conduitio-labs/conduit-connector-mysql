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
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

type Destination struct {
	sdk.UnimplementedDestination
	db     *sqlx.DB
	config common.DestinationConfig
}

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

func (d *Destination) Open(_ context.Context) (err error) {
	d.db, err = sqlx.Open("mysql", d.config.DSN)
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, recs []opencdc.Record) (written int, err error) {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}

	//nolint:errcheck // will always error if committed, no need to check
	defer tx.Rollback()

	for _, rec := range recs {
		switch rec.Operation {
		case opencdc.OperationSnapshot:
			if err := d.upsertRecord(ctx, tx, rec); err != nil {
				return 0, err
			}
		case opencdc.OperationCreate:
			if err := d.upsertRecord(ctx, tx, rec); err != nil {
				return 0, err
			}
		case opencdc.OperationUpdate:
			if err := d.upsertRecord(ctx, tx, rec); err != nil {
				return 0, err
			}
		case opencdc.OperationDelete:
			if err := d.deleteRecord(ctx, tx, rec); err != nil {
				return 0, err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return len(recs), nil
}

func (d *Destination) Teardown(_ context.Context) error {
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}

	return nil
}

func (d *Destination) upsertRecord(ctx context.Context, tx *sqlx.Tx, rec opencdc.Record) error {
	payload, isStructured := rec.Payload.After.(opencdc.StructuredData)
	if !isStructured {
		data := make(opencdc.StructuredData)
		if err := json.Unmarshal(rec.Payload.After.Bytes(), &data); err != nil {
			return fmt.Errorf("failed to json unmarshal non structured data: %w", err)
		}

		payload = data
	}

	columns := make([]string, 0, len(payload))
	values := make([]any, 0, len(payload))

	for col, val := range payload {
		columns = append(columns, col)
		values = append(values, val)
	}

	query := squirrel.Insert(d.config.Table).
		Columns(columns...).
		Values(values...).
		Suffix("ON DUPLICATE KEY UPDATE " + buildUpsertSuffix(payload))

	sql, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	_, err = tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("failed to upsert record: %w", err)
	}

	return nil
}

func buildUpsertSuffix(upsertList opencdc.StructuredData) string {
	parts := make([]string, 0, len(upsertList))
	for col := range upsertList {
		parts = append(parts, fmt.Sprintf("%s = VALUES(%s)", col, col))
	}
	return strings.Join(parts, ", ")
}

func (d *Destination) deleteRecord(ctx context.Context, tx *sqlx.Tx, rec opencdc.Record) error {
	val, err := d.parseRecordKey(rec.Key)
	if err != nil {
		return err
	}

	query := squirrel.
		Delete(d.config.Table).
		Where(squirrel.Eq{d.config.Key: val})

	sql, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	_, err = tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	return nil
}

func (d *Destination) parseRecordKey(key opencdc.Data) (any, error) {
	data := make(opencdc.StructuredData)
	if err := json.Unmarshal(key.Bytes(), &data); err != nil {
		return nil, fmt.Errorf("failed to parse key: %w", err)
	}

	val, ok := data[d.config.Key]
	if !ok {
		return nil, fmt.Errorf("primary key not found")
	}

	return val, nil
}

type recordBatch interface {
	write(ctx context.Context) error
	add(opencdc.Record)
}

type upsertBatch struct {
	recs []opencdc.Record
}

func (b *upsertBatch) write(ctx context.Context) error {
	return nil
}

func (b *upsertBatch) add(rec opencdc.Record) { b.recs = append(b.recs, rec) }

type deleteBatch struct {
	recs []opencdc.Record
}

func (b *deleteBatch) write(ctx context.Context) error {
	return nil
}

func (b *deleteBatch) add(rec opencdc.Record) { b.recs = append(b.recs, rec) }

// batchRecords follows the following pattern:
// https://github.com/conduitio-labs/conduit-connector-sqs/blob/c8c94fc6254cc6f2521f179efffb60aa78d399a0/destination/destination.go#L189-L203
func batchRecords(recs []opencdc.Record) []recordBatch {
	var batches []recordBatch
	for _, rec := range recs {
		switch rec.Operation {
		case opencdc.OperationSnapshot, opencdc.OperationCreate, opencdc.OperationUpdate:
			if len(batches) == 0 {
				batches = append(batches, &upsertBatch{recs: []opencdc.Record{rec}})
				continue
			}

			lastBatch := batches[len(batches)-1]
			if batch, ok := lastBatch.(*upsertBatch); ok {
				batch.add(rec)
				continue
			}

			batches = append(batches, &upsertBatch{recs: []opencdc.Record{rec}})
		case opencdc.OperationDelete:
			if len(batches) == 0 {
				batches = append(batches, &deleteBatch{recs: []opencdc.Record{rec}})
				continue
			}

			lastBatch := batches[len(batches)-1]
			if batch, ok := lastBatch.(*deleteBatch); ok {
				batch.add(rec)
				continue
			}

			batches = append(batches, &deleteBatch{recs: []opencdc.Record{rec}})
		}
	}

	return batches
}
