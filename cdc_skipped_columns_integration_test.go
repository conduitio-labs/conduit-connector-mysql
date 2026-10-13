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
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

const skippedTable = "skipped_cols"

// setupSkippedTable recreates the table and seeds one row (id 1) with a value
// in every nullable column, so a wrongly emitted NULL is distinguishable from
// the real value.
func setupSkippedTable(is *is.I, db testutils.DB) {
	is.Helper()
	is.NoErr(db.Exec("DROP TABLE IF EXISTS " + skippedTable).Error)
	is.NoErr(db.Exec("CREATE TABLE " + skippedTable +
		" (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, note VARCHAR(20) NULL, doc BLOB NULL)").Error)
	is.NoErr(db.Exec("INSERT INTO " + skippedTable + " VALUES (1, 'a', 'keep', 'blob-value')").Error)
}

func skippedCdcIterator(ctx context.Context, t *testing.T, is *is.I) (*cdcIterator, func() error) {
	t.Helper()

	config, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)

	iterator, err := newCdcIterator(ctx, cdcIteratorConfig{
		mysqlConfig:         config,
		tables:              []string{skippedTable},
		tableKeys:           common.TableKeys{skippedTable: {"id"}},
		db:                  testutils.NewDB(t).SqlxDB,
		disableCanalLogging: true,
	})
	is.NoErr(err)
	is.NoErr(iterator.obtainStartPosition())
	is.NoErr(iterator.start(ctx))

	return iterator, func() error { return iterator.Teardown(ctx) }
}

// writerSession returns a connection whose binlog_row_image is set for that
// session only. The server-level value stays FULL, which is the case the
// server-level startup check cannot see.
func writerSession(ctx context.Context, t *testing.T, is *is.I, image string) *sql.Conn {
	t.Helper()
	conn, err := testutils.NewDB(t).SqlxDB.Conn(ctx)
	is.NoErr(err)
	t.Cleanup(func() { conn.Close() })
	_, err = conn.ExecContext(ctx, "SET SESSION binlog_row_image = '"+image+"'")
	is.NoErr(err)
	return conn
}

func execOn(ctx context.Context, is *is.I, conn *sql.Conn, q string) {
	is.Helper()
	_, err := conn.ExecContext(ctx, q)
	is.NoErr(err)
}

func readOne(ctx context.Context, is *is.I, it *cdcIterator) opencdc.Record {
	is.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	recs, err := it.ReadN(ctx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	return recs[0]
}

// readHalt expects ReadN to fail with the skipped-columns error and returns it.
func readHalt(ctx context.Context, t *testing.T, is *is.I, it *cdcIterator) *SkippedColumnsError {
	is.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	recs, err := it.ReadN(ctx, 1)
	if len(recs) > 0 {
		// Without the halt the event is emitted, with the zero value (the
		// connector's NULL encoding) in every column the image left out.
		t.Fatalf("record built from a partial row image: operation=%s before=%v after=%v",
			recs[0].Operation, recs[0].Payload.Before, recs[0].Payload.After)
	}
	is.True(err != nil)
	is.True(errors.Is(err, ErrSkippedColumns))
	var se *SkippedColumnsError
	is.True(errors.As(err, &se))
	is.True(!errors.Is(err, ErrCDCStartPositionUnavailable)) // a halt is not a purged binlog
	return se
}

func TestCDCIterator_SkippedColumns_MinimalUpdateHalts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	setupSkippedTable(is, db)

	it, teardown := skippedCdcIterator(ctx, t, is)

	w := writerSession(ctx, t, is, "MINIMAL")
	// note and doc are nullable and hold values; the UPDATE does not touch them.
	execOn(ctx, is, w, "UPDATE "+skippedTable+" SET name = 'a2' WHERE id = 1")

	se := readHalt(ctx, t, is, it)
	is.Equal(se.Table, "meroxadb."+skippedTable)
	is.Equal(se.Action, "update")
	is.True(strings.HasPrefix(se.BinlogFile, "binlog."))
	is.True(se.EndPos > se.StartPos && se.StartPos > 0)
	for _, col := range []string{"note", "doc"} {
		is.True(strings.Contains(se.Error(), col)) // untouched nullable column must be named
	}
	is.True(strings.Contains(se.Error(), CodeSkippedColumns))

	// The halted event stays in the binlog, so the iterator stays halted.
	readHalt(ctx, t, is, it)

	err := teardown()
	is.True(errors.Is(err, ErrSkippedColumns))
}

func TestCDCIterator_SkippedColumns_RecordsBeforeTheHaltAreDelivered(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	setupSkippedTable(is, db)

	it, teardown := skippedCdcIterator(ctx, t, is)

	is.NoErr(db.Exec("INSERT INTO " + skippedTable + " VALUES (2, 'b', 'n2', 'd2')").Error)
	w := writerSession(ctx, t, is, "MINIMAL")
	execOn(ctx, is, w, "UPDATE "+skippedTable+" SET name = 'b2' WHERE id = 2")

	rec := readOne(ctx, is, it)
	is.Equal(rec.Operation, opencdc.OperationCreate)
	is.NoErr(it.Ack(ctx, rec.Position))
	readHalt(ctx, t, is, it)

	_ = teardown()
}

func TestCDCIterator_SkippedColumns_FullImagePasses(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	setupSkippedTable(is, db)

	it, teardown := skippedCdcIterator(ctx, t, is)
	defer func() { is.NoErr(teardown()) }()

	w := writerSession(ctx, t, is, "FULL")
	execOn(ctx, is, w, "UPDATE "+skippedTable+" SET name = 'a2' WHERE id = 1")

	rec := readOne(ctx, is, it)
	is.Equal(rec.Operation, opencdc.OperationUpdate)
	after := rec.Payload.After.(opencdc.StructuredData)
	is.Equal(after["name"], "a2")
	is.Equal(after["note"], "keep") // the untouched value survives
	is.Equal(after["doc"], []byte("blob-value"))
}

// A real NULL is in the row image, so it must not halt. The connector still
// encodes NULL as the column's zero value today (schema.go formatValue); the
// NULL-semantics change is tracked in ConduitIO/conduit#3040 and is out of
// scope here. What this pins is "delivered, not halted, before-image intact".
func TestCDCIterator_SkippedColumns_RealNullPasses(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	setupSkippedTable(is, db)

	it, teardown := skippedCdcIterator(ctx, t, is)
	defer func() { is.NoErr(teardown()) }()

	w := writerSession(ctx, t, is, "FULL")
	execOn(ctx, is, w, "UPDATE "+skippedTable+" SET note = NULL WHERE id = 1")

	rec := readOne(ctx, is, it)
	is.Equal(rec.Operation, opencdc.OperationUpdate)
	before := rec.Payload.Before.(opencdc.StructuredData)
	after := rec.Payload.After.(opencdc.StructuredData)
	is.Equal(before["note"], "keep")
	is.Equal(after["note"], "") // NULL, in the connector's current zero-value encoding
	is.Equal(after["name"], "a")
	is.Equal(after["doc"], []byte("blob-value"))
}

func TestCDCIterator_SkippedColumns_NoblobHalts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	setupSkippedTable(is, db)

	it, teardown := skippedCdcIterator(ctx, t, is)

	w := writerSession(ctx, t, is, "NOBLOB")
	// The BLOB column is not touched, so NOBLOB leaves it out of both images.
	execOn(ctx, is, w, "UPDATE "+skippedTable+" SET name = 'a2' WHERE id = 1")

	se := readHalt(ctx, t, is, it)
	is.Equal(se.Action, "update")
	is.Equal(se.Columns, []string{"doc"})

	_ = teardown()
}

func TestCDCIterator_SkippedColumns_MinimalDeleteHalts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	setupSkippedTable(is, db)

	it, teardown := skippedCdcIterator(ctx, t, is)

	w := writerSession(ctx, t, is, "MINIMAL")
	execOn(ctx, is, w, "DELETE FROM "+skippedTable+" WHERE id = 1")

	se := readHalt(ctx, t, is, it)
	is.Equal(se.Action, "delete")

	_ = teardown()
}

// MINIMAL and NOBLOB log a full after image for INSERT, so a partial-image
// session that only inserts must not be halted (no false halt).
func TestCDCIterator_SkippedColumns_PartialImageInsertPasses(t *testing.T) {
	for _, image := range []string{"MINIMAL", "NOBLOB"} {
		t.Run(image, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			db := testutils.NewDB(t)
			setupSkippedTable(is, db)

			it, teardown := skippedCdcIterator(ctx, t, is)
			defer func() { is.NoErr(teardown()) }()

			w := writerSession(ctx, t, is, image)
			execOn(ctx, is, w, "INSERT INTO "+skippedTable+" VALUES (3, 'c', 'n3', 'd3')")

			rec := readOne(ctx, is, it)
			is.Equal(rec.Operation, opencdc.OperationCreate)
			after := rec.Payload.After.(opencdc.StructuredData)
			is.Equal(after["note"], "n3")
			is.Equal(after["doc"], []byte("d3"))
		})
	}
}

// Virtual, stored and invisible columns are part of a FULL image. If MySQL
// listed any of them as skipped, every FULL pipeline on such a table would
// halt, so pin that it does not.
func TestCDCIterator_SkippedColumns_FullImageGeneratedAndInvisibleColumnsPass(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS " + skippedTable).Error)
	is.NoErr(db.Exec("CREATE TABLE " + skippedTable + " (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, " +
		"vg INT AS (id * 2) VIRTUAL, sg INT AS (id * 3) STORED, inv INT INVISIBLE DEFAULT 5)").Error)
	is.NoErr(db.Exec("INSERT INTO " + skippedTable + " (id, name) VALUES (1, 'a')").Error)

	it, teardown := skippedCdcIterator(ctx, t, is)
	defer func() { is.NoErr(teardown()) }()

	is.NoErr(db.Exec("UPDATE " + skippedTable + " SET name = 'z' WHERE id = 1").Error)
	is.NoErr(db.Exec("DELETE FROM " + skippedTable + " WHERE id = 1").Error)

	rec := readOne(ctx, is, it)
	is.Equal(rec.Operation, opencdc.OperationUpdate)
	after := rec.Payload.After.(opencdc.StructuredData)
	is.Equal(after["name"], "z")
	is.Equal(after["sg"], int32(3))

	rec = readOne(ctx, is, it)
	is.Equal(rec.Operation, opencdc.OperationDelete)
}
