// Copyright © 2026 Meroxa, Inc.
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
	"errors"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func columnsTestRecord(table string, after opencdc.StructuredData) opencdc.Record {
	metadata := opencdc.Metadata{}
	metadata.SetCollection(table)
	return opencdc.Record{
		Operation: opencdc.OperationCreate,
		Metadata:  metadata,
		Key:       opencdc.StructuredData{"id": after["id"]},
		Payload:   opencdc.Change{After: after},
	}
}

type columnsRow struct {
	ID    int64
	V     *string
	Extra *string
}

func readColumnsRows(is *is.I, db testutils.DB, table string) map[int64]columnsRow {
	var rows []columnsRow
	is.NoErr(db.Raw("SELECT id, v, extra FROM " + table + " ORDER BY id").Scan(&rows).Error)
	out := make(map[int64]columnsRow, len(rows))
	for _, r := range rows {
		out[r.ID] = r
	}
	return out
}

func strp(s string) *string { return &s }

// Regression test for #222. The column list of an upsert came from the first
// record of a batch, so a field that only a later record had was dropped and
// the record acked anyway (invariant 6).
func TestDestination_Write_FieldMissingFromFirstRecordIsWritten(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_cols").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_cols (id BIGINT PRIMARY KEY, v VARCHAR(10), extra VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_cols") })

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	written, err := dest.Write(ctx, []opencdc.Record{
		columnsTestRecord("dest_cols", opencdc.StructuredData{"id": 1, "v": "a"}),
		columnsTestRecord("dest_cols", opencdc.StructuredData{"id": 2, "v": "b", "extra": "x"}),
	})
	is.NoErr(err)
	is.Equal(written, 2)

	rows := readColumnsRows(is, db, "dest_cols")
	is.Equal(len(rows), 2)
	is.Equal(rows[1], columnsRow{ID: 1, V: strp("a")})                   // the first record had no extra field
	is.Equal(rows[2], columnsRow{ID: 2, V: strp("b"), Extra: strp("x")}) // the second one's must not be dropped
}

// A field the table has no column for fails the whole write (the reject
// schema-evolution policy), wherever it is in the batch, and nothing of the
// batch is stored.
func TestDestination_Write_UnknownFieldFailsTheWrite(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_cols_unknown").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_cols_unknown (id BIGINT PRIMARY KEY, v VARCHAR(10), extra VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_cols_unknown") })

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	for name, batch := range map[string][]opencdc.Record{
		"unknown field in a later record": {
			columnsTestRecord("dest_cols_unknown", opencdc.StructuredData{"id": 1, "v": "a"}),
			columnsTestRecord("dest_cols_unknown", opencdc.StructuredData{"id": 2, "v": "b", "nope": "x"}),
		},
		"unknown field in the first record": {
			columnsTestRecord("dest_cols_unknown", opencdc.StructuredData{"id": 1, "v": "a", "nope": "x"}),
			columnsTestRecord("dest_cols_unknown", opencdc.StructuredData{"id": 2, "v": "b"}),
		},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			written, err := dest.Write(ctx, batch)
			is.True(errors.Is(err, ErrUnknownColumn))
			var coded *Error
			is.True(errors.As(err, &coded))
			is.Equal(coded.Code, CodeUnknownColumn)
			is.Equal(written, 0)
			is.Equal(len(readColumnsRows(is, db, "dest_cols_unknown")), 0)
		})
	}
}

// Records of one key in different column runs are applied in write order: the
// last record wins, and a field it lacks keeps the value an earlier record set.
func TestDestination_Write_ColumnRunsKeepOrder(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_cols_order").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_cols_order (id BIGINT PRIMARY KEY, v VARCHAR(10), extra VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_cols_order") })

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	written, err := dest.Write(ctx, []opencdc.Record{
		columnsTestRecord("dest_cols_order", opencdc.StructuredData{"id": 1, "v": "a"}),
		columnsTestRecord("dest_cols_order", opencdc.StructuredData{"id": 1, "v": "b", "extra": "x"}),
		columnsTestRecord("dest_cols_order", opencdc.StructuredData{"id": 1, "v": "c"}),
	})
	is.NoErr(err)
	is.Equal(written, 3)

	rows := readColumnsRows(is, db, "dest_cols_order")
	is.Equal(rows[1], columnsRow{ID: 1, V: strp("c"), Extra: strp("x")})
}
