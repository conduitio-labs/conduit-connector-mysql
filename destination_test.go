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
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestDestination_Teardown(t *testing.T) {
	is := is.New(t)
	con := NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestBatchRecords(t *testing.T) {
	testRec := func(table string, op opencdc.Operation) opencdc.Record {
		metadata := opencdc.Metadata{}
		metadata.SetCollection(table)

		return opencdc.Record{
			Operation: op,
			Metadata:  metadata,
		}
	}

	t.Run("empty slice returns nil batches", func(t *testing.T) {
		is := is.New(t)

		batches, err := batchRecords(nil)
		is.NoErr(err)
		is.Equal(batches, nil)
	})

	t.Run("single record creates a single batch", func(t *testing.T) {
		is := is.New(t)

		rec := testRec("table1", opencdc.OperationCreate)
		batches, err := batchRecords([]opencdc.Record{rec})
		is.NoErr(err)
		is.Equal(len(batches), 1)
		is.Equal(batches[0].kind, upsertBatchKind)
		is.Equal(batches[0].table, "table1")
		is.Equal(len(batches[0].recs), 1)
	})

	t.Run("multiple records with same operation and table are batched together", func(t *testing.T) {
		is := is.New(t)

		rec1 := testRec("table1", opencdc.OperationCreate)
		rec2 := testRec("table1", opencdc.OperationCreate)
		rec3 := testRec("table1", opencdc.OperationCreate)
		batches, err := batchRecords([]opencdc.Record{rec1, rec2, rec3})
		is.NoErr(err)
		is.Equal(len(batches), 1)
		is.Equal(batches[0].kind, upsertBatchKind)
		is.Equal(batches[0].table, "table1")
		is.Equal(len(batches[0].recs), 3)
	})

	t.Run("records with different operations are split into separate batches", func(t *testing.T) {
		is := is.New(t)

		rec1 := testRec("table1", opencdc.OperationCreate)
		rec2 := testRec("table1", opencdc.OperationDelete)
		rec3 := testRec("table1", opencdc.OperationCreate)
		batches, err := batchRecords([]opencdc.Record{rec1, rec2, rec3})
		is.NoErr(err)
		is.Equal(len(batches), 3)
		is.Equal(batches[0].kind, upsertBatchKind)
		is.Equal(batches[1].kind, deleteBatchKind)
		is.Equal(batches[2].kind, upsertBatchKind)
	})

	t.Run("records with different tables are split into separate batches", func(t *testing.T) {
		is := is.New(t)

		rec1 := testRec("table1", opencdc.OperationCreate)
		rec2 := testRec("table2", opencdc.OperationCreate)
		rec3 := testRec("table1", opencdc.OperationCreate)
		batches, err := batchRecords([]opencdc.Record{rec1, rec2, rec3})
		is.NoErr(err)
		is.Equal(len(batches), 3)
		is.Equal(batches[0].table, "table1")
		is.Equal(batches[1].table, "table2")
		is.Equal(batches[2].table, "table1")
	})

	t.Run("error on an operation the destination does not handle", func(t *testing.T) {
		is := is.New(t)

		// Skipping the record would still count it as written.
		_, err := batchRecords([]opencdc.Record{
			testRec("table1", opencdc.OperationCreate),
			testRec("table1", opencdc.Operation(0)),
		})
		is.True(errors.Is(err, ErrUnsupportedOperation))
		var coded *Error
		is.True(errors.As(err, &coded))
		is.Equal(coded.Code, CodeUnsupportedOperation)

		_, err = batchRecords([]opencdc.Record{testRec("table1", opencdc.Operation(0))})
		is.True(errors.Is(err, ErrUnsupportedOperation))
	})

	t.Run("error when collection metadata is missing", func(t *testing.T) {
		is := is.New(t)

		rec := opencdc.Record{}
		_, err := batchRecords([]opencdc.Record{rec})
		is.True(err != nil)
	})
}

func TestColumnRuns(t *testing.T) {
	rec := func(after opencdc.Data) opencdc.Record {
		return opencdc.Record{Operation: opencdc.OperationCreate, Payload: opencdc.Change{After: after}}
	}

	t.Run("consecutive records with the same fields share a run, order is kept", func(t *testing.T) {
		is := is.New(t)
		recs := []opencdc.Record{
			rec(opencdc.StructuredData{"id": 1, "v": "a"}),
			rec(opencdc.StructuredData{"v": "b", "id": 2}),
			rec(opencdc.StructuredData{"id": 3, "v": "c", "extra": "x"}),
			rec(opencdc.StructuredData{"id": 4, "v": "d"}),
		}
		runs, err := columnRuns(recs)
		is.NoErr(err)
		is.Equal(len(runs), 3)
		is.Equal(runs[0].columns, []string{"id", "v"})
		is.Equal(runs[1].columns, []string{"extra", "id", "v"})
		is.Equal(runs[2].columns, []string{"id", "v"})
		is.Equal(len(runs[0].recs), 2)
		var got []any
		for _, r := range runs {
			for _, rr := range r.recs {
				got = append(got, rr.Payload.After.(opencdc.StructuredData)["id"])
			}
		}
		is.Equal(got, []any{1, 2, 3, 4})
	})

	t.Run("raw payload is refused", func(t *testing.T) {
		is := is.New(t)
		_, err := columnRuns([]opencdc.Record{rec(opencdc.RawData(`{"id":1}`))})
		is.True(errors.Is(err, ErrPayloadNotStructured))
	})

	t.Run("empty payload is refused", func(t *testing.T) {
		is := is.New(t)
		_, err := columnRuns([]opencdc.Record{rec(opencdc.StructuredData{})})
		is.True(errors.Is(err, ErrPayloadEmpty))
	})
}

func TestQuoteIdentifiers(t *testing.T) {
	is := is.New(t)
	is.Equal(quoteIdent("order"), "`order`")
	is.Equal(quoteIdent("we`ird"), "`we``ird`")
	is.Equal(quoteIdent("a`) DROP"), "`a``) DROP`")
	is.Equal(quoteTable("t"), "`t`")
	is.Equal(quoteTable("db.t"), "`db`.`t`")
	is.Equal(quoteTable("db.t.x"), "`db`.`t.x`")
}
