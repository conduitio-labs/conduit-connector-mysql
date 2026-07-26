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
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
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

	t.Run("error when collection metadata is missing", func(t *testing.T) {
		is := is.New(t)

		rec := opencdc.Record{}
		_, err := batchRecords([]opencdc.Record{rec})
		is.True(err != nil)
	})

	// Regression tests for the CDC cold-start checkpoint record (see
	// cdcIterator.startColdStart in this package): it carries no Collection
	// metadata (by design; it isn't real table data) and, pre-fix, would have
	// hit exactly the "error when collection metadata is missing" case above -
	// a source(cold-start)->destination pipeline using this repo's own
	// connectors on both ends would error on Write instead of silently
	// no-op'ing the record. filterCheckpointRecords must drop it before
	// GetCollection is ever called on it.

	checkpointRec := func() opencdc.Record {
		metadata := opencdc.Metadata{common.CheckpointMetadataKey: "true"}
		return opencdc.Record{
			Operation: opencdc.OperationDelete,
			Metadata:  metadata,
			Key:       opencdc.RawData("mysql.checkpoint.binlog.000001.4"), // not valid JSON - parseRecordKey must never see it
		}
	}

	t.Run("checkpoint-only batch produces no batches and no error", func(t *testing.T) {
		is := is.New(t)

		batches, err := batchRecords([]opencdc.Record{checkpointRec()})
		is.NoErr(err)
		is.Equal(len(batches), 0)
	})

	t.Run("checkpoint record is dropped from a mixed batch, real record survives", func(t *testing.T) {
		is := is.New(t)

		realRec := testRec("table1", opencdc.OperationCreate)
		batches, err := batchRecords([]opencdc.Record{checkpointRec(), realRec})
		is.NoErr(err)
		is.Equal(len(batches), 1)
		is.Equal(batches[0].kind, upsertBatchKind)
		is.Equal(batches[0].table, "table1")
		is.Equal(len(batches[0].recs), 1)
	})
}
