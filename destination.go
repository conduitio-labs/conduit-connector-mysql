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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type DestinationConfig struct {
	sdk.DefaultDestinationMiddleware

	Config

	mysqlCfg        *mysql.Config
	tableKeyFetcher *common.TableKeyFetcher
}

func (d *DestinationConfig) MysqlCfg() *mysql.Config {
	return d.mysqlCfg
}

func (d *DestinationConfig) TableKeyFetcher() *common.TableKeyFetcher {
	return d.tableKeyFetcher
}

func (d *DestinationConfig) Validate(context.Context) error {
	mysqlCfg, err := mysql.ParseDSN(d.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	d.mysqlCfg = mysqlCfg
	d.tableKeyFetcher = common.NewTableKeyFetcher(mysqlCfg.DBName)

	return nil
}

type Destination struct {
	sdk.UnimplementedDestination
	db     *sqlx.DB
	config DestinationConfig
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

// Write applies recs in a single transaction.
//
// Invariant 1: the returned count is the number of records that are durably
// committed. The transaction is all-or-nothing, so the result is either
// len(recs) with a nil error, or 0 with an error. Reporting a partial count after
// a failure would make the SDK acknowledge records that the rollback discarded.
func (d *Destination) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}

	//nolint:errcheck // will always error if committed, no need to check
	defer tx.Rollback()

	batches, err := batchRecords(recs)
	if err != nil {
		return 0, fmt.Errorf("failed to batch records: %w", err)
	}

	for _, batch := range batches {
		var err error
		switch batch.kind {
		case upsertBatchKind:
			err = d.upsertRecords(ctx, tx, batch.table, batch.recs)
		case deleteBatchKind:
			_, err = d.deleteRecords(ctx, tx, batch.table, batch.recs)
		}
		if err != nil {
			// Invariant 1: the deferred Rollback discards every batch applied so
			// far in this call, so none of the records count as written.
			return 0, fmt.Errorf(`failed to process "%s" batch: %w`, batch.kind, err)
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

// upsertRecords writes recs with INSERT ... ON DUPLICATE KEY UPDATE.
//
// Invariant 6: every record is written with exactly the fields it carries. The
// records are split into runs of consecutive records that have the same field
// set, and each run is one statement. Building one column list for the whole
// batch from its first record silently dropped any field a later record had
// and the first one lacked (#222). Runs keep the records in their order, so
// when two records in a batch share a key the later one still wins.
//
// A field the table has no column for fails the statement (MySQL error 1054),
// and with it the whole Write: the destination does not alter tables, and
// writing the record without the field would lose it. A field the record lacks
// is not written: an inserted row gets the column default, an updated row keeps
// the column's current value.
func (d *Destination) upsertRecords(ctx context.Context, tx *sqlx.Tx, table string, recs []opencdc.Record) error {
	runs, err := columnRuns(recs)
	if err != nil {
		return err
	}

	for _, run := range runs {
		insert := squirrel.Insert(table).Columns(run.columns...)
		for _, rec := range run.recs {
			payload := rec.Payload.After.(opencdc.StructuredData) // checked by columnRuns
			values := make([]any, 0, len(run.columns))
			for _, col := range run.columns {
				values = append(values, payload[col])
			}
			insert = insert.Values(values...)
		}

		updateParts := make([]string, 0, len(run.columns))
		for _, col := range run.columns {
			updateParts = append(updateParts, fmt.Sprintf("%s=VALUES(%s)", col, col))
		}
		insert = insert.Suffix("ON DUPLICATE KEY UPDATE " + strings.Join(updateParts, ", "))

		sql, args, err := insert.ToSql()
		if err != nil {
			return fmt.Errorf("failed to build query: %w", err)
		}

		if _, err := tx.ExecContext(ctx, sql, args...); err != nil {
			var myErr *mysql.MySQLError
			if errors.As(err, &myErr) && myErr.Number == mysqlErrBadFieldError {
				return newError(ErrUnknownColumn, CodeUnknownColumn, ".Payload.After",
					"add the column to the table before records with the field arrive (the destination does not alter tables), "+
						"or remove the field before the destination with a field.exclude processor",
					"table %q has no column for a field of the records: %s", table, myErr.Message)
			}
			return fmt.Errorf("failed to execute batch upsert: %w", err)
		}
	}

	return nil
}

// keyString renders a record key for error messages. A nil key is valid input
// and must not panic.
func keyString(rec opencdc.Record) string {
	if rec.Key == nil {
		return ""
	}
	return string(rec.Key.Bytes())
}

// mysqlErrBadFieldError is ER_BAD_FIELD_ERROR, "Unknown column".
const mysqlErrBadFieldError = 1054

// upsertRun is a run of consecutive records with the same field set.
type upsertRun struct {
	columns []string // sorted field names, shared by every record of the run
	recs    []opencdc.Record
}

// columnRuns splits recs into runs of consecutive records with the same field
// set. It never reorders records. It fails if a record has no structured
// payload or no fields at all.
func columnRuns(recs []opencdc.Record) ([]upsertRun, error) {
	var runs []upsertRun
	for _, rec := range recs {
		payload, ok := rec.Payload.After.(opencdc.StructuredData)
		if !ok {
			return nil, newError(ErrPayloadNotStructured, CodePayloadNotStructured, ".Payload.After",
				"decode the payload into structured data before the destination, for JSON with a json.decode processor on .Payload.After",
				"record %q has a %T payload, the destination writes only structured data", keyString(rec), rec.Payload.After)
		}
		if len(payload) == 0 {
			return nil, newError(ErrPayloadEmpty, CodePayloadEmpty, ".Payload.After",
				"make sure create, update and snapshot records carry the row in .Payload.After",
				"record %q has no fields to write", keyString(rec))
		}

		columns := make([]string, 0, len(payload))
		for col := range payload {
			columns = append(columns, col)
		}
		slices.Sort(columns)

		if n := len(runs); n > 0 && slices.Equal(runs[n-1].columns, columns) {
			runs[n-1].recs = append(runs[n-1].recs, rec)
			continue
		}
		runs = append(runs, upsertRun{columns: columns, recs: []opencdc.Record{rec}})
	}
	return runs, nil
}

func (d *Destination) deleteRecords(ctx context.Context, tx *sqlx.Tx, table string, recs []opencdc.Record) (int, error) {
	if len(recs) == 0 {
		return 0, nil
	}

	primaryKeys, err := d.config.TableKeyFetcher().GetKeys(tx, table)
	if err != nil {
		return 0, fmt.Errorf("failed to get keys when deleting record batch: %w", err)
	}
	if len(primaryKeys) == 0 {
		// Without this check the empty key list builds a predicate that matches
		// every row, and the delete wipes the table.
		return 0, newError(ErrDeleteWithoutPrimaryKey, CodeDeleteWithoutPrimaryKey, collectionPath,
			"add a primary key to the table, or drop delete records before the destination (a filter processor on .Operation)",
			"cannot delete from table %q: it has no primary key, so a delete record cannot be matched to a row", table)
	}

	if len(primaryKeys) == 1 {
		// single key

		keyName := primaryKeys[0]

		values := make([]any, 0, len(recs))
		for _, rec := range recs {
			val, err := d.parseRecordKey(keyName, rec.Key)
			if err != nil {
				return 0, fmt.Errorf("failed to parse key: %w", err)
			}
			values = append(values, val)
		}

		query := squirrel.
			Delete(table).
			Where(squirrel.Eq{keyName: values})

		sql, args, err := query.ToSql()
		if err != nil {
			return 0, fmt.Errorf("failed to build query: %w", err)
		}

		_, err = tx.ExecContext(ctx, sql, args...)
		if err != nil {
			return 0, fmt.Errorf("failed to execute batch delete: %w", err)
		}

		return len(recs), nil
	}

	// multiple keys

	orConditions := squirrel.Or{}
	for _, rec := range recs {
		andConditions := squirrel.And{}
		for _, keyName := range primaryKeys {
			val, err := d.parseRecordKey(keyName, rec.Key)
			if err != nil {
				return 0, fmt.Errorf("failed to parse key: %w", err)
			}
			andConditions = append(andConditions, squirrel.Eq{keyName: val})
		}
		orConditions = append(orConditions, andConditions)
	}

	query := squirrel.Delete(table).Where(orConditions)
	sql, args, err := query.ToSql()
	if err != nil {
		return 0, fmt.Errorf("failed to build query: %w", err)
	}

	_, err = tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute batch delete: %w", err)
	}

	return len(recs), nil
}

func (d *Destination) parseRecordKey(keyName string, keyVal opencdc.Data) (any, error) {
	data := make(opencdc.StructuredData)
	if err := json.Unmarshal(keyVal.Bytes(), &data); err != nil {
		return nil, fmt.Errorf("failed to parse key: %w", err)
	}

	val, ok := data[keyName]
	if !ok {
		return nil, fmt.Errorf("primary key not found")
	}

	return val, nil
}

// collectionPath is the record field that names the target table. Destination
// errors about a table report it as their path: the destination has no table
// setting, the table comes from each record.
const collectionPath = `.Metadata["opencdc.collection"]`

type recordBatchKind string

const (
	upsertBatchKind recordBatchKind = "upsert"
	deleteBatchKind recordBatchKind = "delete"
)

func newRecordKind(op opencdc.Operation) recordBatchKind {
	switch op {
	case opencdc.OperationSnapshot, opencdc.OperationCreate, opencdc.OperationUpdate:
		return upsertBatchKind
	case opencdc.OperationDelete:
		return deleteBatchKind
	}
	return ""
}

// recordKind fails on an operation the destination does not handle. Skipping such
// a record would still count it as written (invariant 3: never drop a record
// without delivering it).
func recordKind(rec opencdc.Record) (recordBatchKind, error) {
	kind := newRecordKind(rec.Operation)
	if kind == "" {
		return "", newError(ErrUnsupportedOperation, CodeUnsupportedOperation, ".Operation",
			"send only create, update, delete or snapshot records to this destination",
			"the destination does not apply records with operation %q", rec.Operation)
	}
	return kind, nil
}

type recordBatch struct {
	kind  recordBatchKind
	table string
	recs  []opencdc.Record
}

func batchRecords(recs []opencdc.Record) ([]recordBatch, error) {
	if len(recs) == 0 {
		return nil, nil
	}

	firstRec := recs[0]
	table, err := firstRec.Metadata.GetCollection()
	if err != nil {
		return nil, fmt.Errorf("failed to get collection: %w", err)
	}

	var batches []recordBatch
	kind, err := recordKind(firstRec)
	if err != nil {
		return nil, err
	}
	currBatch := recordBatch{
		kind:  kind,
		table: table,
		recs:  []opencdc.Record{firstRec},
	}

	recs = recs[1:]

	for _, rec := range recs {
		table, err := rec.Metadata.GetCollection()
		if err != nil {
			return nil, fmt.Errorf("failed to get collection: %w", err)
		}

		kind, err := recordKind(rec)
		if err != nil {
			return nil, err
		}
		if currBatch.kind == kind && currBatch.table == table {
			currBatch.recs = append(currBatch.recs, rec)
		} else {
			batches = append(batches, currBatch)
			currBatch = recordBatch{
				kind:  kind,
				table: table,
				recs:  []opencdc.Record{rec},
			}
		}
	}

	batches = append(batches, currBatch)

	return batches, nil
}
