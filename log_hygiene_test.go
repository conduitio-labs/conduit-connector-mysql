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
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/iskorotkov/avro/v2"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// A primary key value is record data. None of the fetch worker's log lines
// may carry it, at any level.
const secretKeyValue = "SECRET-customer-4711"

// logFakeDriver answers every query with the rows the test set, or fails it.
type logFakeDriver struct {
	cols []string
	rows [][]driver.Value
	err  error
}

func (d *logFakeDriver) Open(string) (driver.Conn, error) { return &logFakeConn{d}, nil }

type logFakeConn struct{ d *logFakeDriver }

func (c *logFakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *logFakeConn) Close() error                        { return nil }
func (c *logFakeConn) Begin() (driver.Tx, error)           { return logFakeTx{}, nil }
func (c *logFakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.d.err != nil {
		return nil, c.d.err
	}
	return &logFakeRows{cols: c.d.cols, rows: c.d.rows}, nil
}

type logFakeTx struct{}

func (logFakeTx) Commit() error   { return nil }
func (logFakeTx) Rollback() error { return nil }

type logFakeRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *logFakeRows) Columns() []string { return r.cols }
func (r *logFakeRows) Close() error      { return nil }
func (r *logFakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

func newLogFakeDB(t *testing.T, d *logFakeDriver) *sqlx.DB {
	t.Helper()
	db := sqlx.NewDb(sql.OpenDB(logFakeConnector{d}), "mysql")
	t.Cleanup(func() { db.Close() })
	return db
}

type logFakeConnector struct{ d *logFakeDriver }

func (c logFakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &logFakeConn{c.d}, nil
}
func (c logFakeConnector) Driver() driver.Driver { return c.d }

func logCtx() (context.Context, *bytes.Buffer) {
	var buf bytes.Buffer
	return zerolog.New(&buf).Level(zerolog.TraceLevel).WithContext(context.Background()), &buf
}

func TestFetchWorkerSingleKey_LogsDoNotContainKeyValues(t *testing.T) {
	is := is.New(t)
	ctx, buf := logCtx()

	cfg := fetchWorkerConfig{
		table:     "orders",
		fetchSize: 10,
		db: newLogFakeDB(t, &logFakeDriver{
			cols: []string{"min_value", "max_value"},
			rows: [][]driver.Value{{secretKeyValue + "-min", secretKeyValue + "-max"}},
		}),
	}
	w := &fetchWorkerSingleKey{config: cfg, primaryKey: "id", lastPosition: common.TablePositionSingleKey{LastRead: secretKeyValue + "-last"}}

	empty, err := w.fetchStartEnd(ctx)
	is.NoErr(err)
	is.True(!empty)

	// the query logs its arguments, which are the bounds
	w.config.db = newLogFakeDB(t, &logFakeDriver{err: errors.New("injected")})
	_, err = w.selectRowsChunk(ctx, secretKeyValue+"-chunk", true)
	is.True(err != nil)

	out := buf.String()
	is.True(strings.Contains(out, "fetched start and end"))
	is.True(strings.Contains(out, "created query"))
	is.True(strings.Contains(out, `"start_hash"`))
	is.True(!strings.Contains(out, "SECRET"))
}

func TestFetchWorkerMultipleKey_LogsDoNotContainKeyValues(t *testing.T) {
	is := is.New(t)
	ctx, buf := logCtx()

	db := newLogFakeDB(t, &logFakeDriver{err: errors.New("injected")})
	tx, err := db.BeginTxx(ctx, nil)
	is.NoErr(err)
	defer tx.Rollback()

	w := &fetchWorkerMultipleKey{config: fetchWorkerByKeysConfig{fetchWorkerConfig: fetchWorkerConfig{
		db: db, table: "orders", fetchSize: 10, primaryKeys: common.PrimaryKeys{"a", "b"},
	}}}
	start := common.TablePositionMultipleKey{
		{KeyName: "a", LastRead: secretKeyValue + "-a"},
		{KeyName: "b", LastRead: secretKeyValue + "-b"},
	}
	_, err = w.selectRowsChunk(ctx, tx, start, true)
	is.True(err != nil)

	out := buf.String()
	is.True(strings.Contains(out, "created query"))
	is.True(strings.Contains(out, `"args"`))
	is.True(!strings.Contains(out, "SECRET"))
}

func TestLogHash(t *testing.T) {
	is := is.New(t)
	h := logHash(secretKeyValue)
	is.Equal(len(h), 16)
	is.True(!strings.Contains(h, "SECRET"))
	is.Equal(h, logHash(secretKeyValue))
	is.True(h != logHash(secretKeyValue+"x"))
	is.True(logHash(int64(1)) != logHash("1")) // type matters
	is.Equal(len(logHash(nil)), 16)
}

// TestFormatValue_OutOfRangeWarningDoesNotContainValue: the warning for an
// integer that does not fit an avro int names the column and the Go type, not
// the value.
func TestFormatValue_OutOfRangeWarningDoesNotContainValue(t *testing.T) {
	is := is.New(t)
	ctx, buf := logCtx()

	s := newSchemaMapper()
	s.colTypes = map[string]*avroNamedType{"qty": {Name: "qty", avroType: avroType{Type: avro.Int}}}

	for _, v := range []any{int(9876543210123), int64(9876543210123), uint32(4294967295), uint64(9876543210123)} {
		_, err := s.formatValue(ctx, "qty", v)
		is.NoErr(err)
	}

	out := buf.String()
	is.True(strings.Contains(out, "cannot be encoded in avro int"))
	is.True(strings.Contains(out, "qty"))
	is.True(!strings.Contains(out, "9876543210123"))
	is.True(!strings.Contains(out, "4294967295"))
}

// Errors reach the DLQ, the API and CLI output, so a row value is never in
// them: not the digits the driver returned for a BIGINT UNSIGNED, and not the
// value that overflows.
func TestFormatValue_ErrorsDoNotContainTheValue(t *testing.T) {
	ctx := context.Background()
	s := newSchemaMapper()
	s.table = "orders"
	s.colTypes = map[string]*avroNamedType{"qty": {Name: "qty", avroType: avroType{Type: avro.Long}}}

	for name, v := range map[string]any{
		"digits with garbage": []uint8("99999999999999999999SECRET"),
		"uint64 overflow":     uint64(18446744073709551615),
		"bytes overflow":      []uint8("18446744073709551615"),
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			_, err := s.formatValue(ctx, "qty", v)
			is.True(err != nil)
			is.True(strings.Contains(err.Error(), "orders.qty"))
			is.True(!strings.Contains(err.Error(), "SECRET"))
			is.True(!strings.Contains(err.Error(), "99999999999999999999"))
			is.True(!strings.Contains(err.Error(), "18446744073709551615"))
		})
	}
}
