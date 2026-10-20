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
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/matryer/is"
)

func tableOf(cols ...schema.TableColumn) *schema.Table {
	return &schema.Table{Schema: "db", Name: "t", Columns: cols}
}

func col(name string, typ int) schema.TableColumn {
	return schema.TableColumn{Name: name, Type: typ}
}

func tableMapOf(types []byte, names ...string) *replication.TableMapEvent {
	tm := &replication.TableMapEvent{ColumnType: types, ColumnMeta: make([]uint16, len(types))}
	for _, n := range names {
		tm.ColumnName = append(tm.ColumnName, []byte(n))
	}
	return tm
}

func TestTableMapMismatch(t *testing.T) {
	// The review's repro: id BIGINT, a INT, b VARCHAR, d INT INVISIBLE was
	// written; the table is now id, a, d INT INVISIBLE, e INT.
	written := tableMapOf([]byte{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_LONG})
	enumMeta := tableMapOf([]byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_STRING})
	enumMeta.ColumnMeta = []uint16{uint16(mysql.MYSQL_TYPE_ENUM) << 8, uint16(mysql.MYSQL_TYPE_SET) << 8}

	tests := []struct {
		name  string
		tm    *replication.TableMapEvent
		table *schema.Table
		want  string // substring of the mismatch; "" for compatible
	}{
		{
			name:  "no table map",
			tm:    nil,
			table: tableOf(col("id", schema.TYPE_NUMBER)),
		},
		{
			name:  "same columns",
			tm:    written,
			table: tableOf(col("id", schema.TYPE_NUMBER), col("a", schema.TYPE_NUMBER), col("b", schema.TYPE_STRING), col("d", schema.TYPE_NUMBER)),
		},
		{
			name:  "review repro: DROP b, ADD e keeps the width, the type at 3 changes",
			tm:    written,
			table: tableOf(col("id", schema.TYPE_NUMBER), col("a", schema.TYPE_NUMBER), col("d", schema.TYPE_NUMBER), col("e", schema.TYPE_NUMBER)),
			want:  `column 3 ("d" in the table) is a string or binary in the event and an integer in the table`,
		},
		{
			name:  "width differs",
			tm:    written,
			table: tableOf(col("id", schema.TYPE_NUMBER)),
			want:  "the event has 4 columns, the table has 1",
		},
		{
			name:  "widening an integer or a varchar is compatible",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR}),
			table: tableOf(col("a", schema.TYPE_NUMBER), col("b", schema.TYPE_STRING)),
		},
		{
			name:  "mediumint is an integer",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_INT24}),
			table: tableOf(col("a", schema.TYPE_MEDIUM_INT)),
		},
		{
			name:  "temporal types, both versions",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_DATETIME2, mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_NEWDATE, mysql.MYSQL_TYPE_TIME2}),
			table: tableOf(col("a", schema.TYPE_DATETIME), col("b", schema.TYPE_TIMESTAMP), col("c", schema.TYPE_DATE), col("d", schema.TYPE_TIME)),
		},
		{
			name:  "datetime against timestamp",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_DATETIME2}),
			table: tableOf(col("a", schema.TYPE_TIMESTAMP)),
			want:  "a datetime in the event and a timestamp in the table",
		},
		{
			name:  "enum and set are logged as STRING with the real type in the metadata",
			tm:    enumMeta,
			table: tableOf(col("e", schema.TYPE_ENUM), col("s", schema.TYPE_SET)),
		},
		{
			name:  "enum against a char",
			tm:    enumMeta,
			table: tableOf(col("e", schema.TYPE_STRING), col("s", schema.TYPE_SET)),
			want:  "an enum in the event and a string or binary in the table",
		},
		{
			name:  "text, blob, binary and geometry are all strings or binary",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_BLOB, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_GEOMETRY, mysql.MYSQL_TYPE_JSON}),
			table: tableOf(col("a", schema.TYPE_STRING), col("b", schema.TYPE_BINARY), col("c", schema.TYPE_POINT), col("d", schema.TYPE_JSON)),
		},
		{
			name:  "a type this does not know compares equal",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_NULL}),
			table: tableOf(col("a", schema.TYPE_NUMBER)),
		},
		{
			name:  "names match, case-insensitively",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, "id", "Total"),
			table: tableOf(col("ID", schema.TYPE_NUMBER), col("total", schema.TYPE_NUMBER)),
		},
		{
			name:  "binlog_row_metadata=FULL: same type, other name",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, "id", "b"),
			table: tableOf(col("id", schema.TYPE_NUMBER), col("e", schema.TYPE_NUMBER)),
			want:  `column 2 is "b" in the event and "e" in the table`,
		},
		{
			name:  "names are not compared when the event has fewer names than columns",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_LONG}, "id"),
			table: tableOf(col("id", schema.TYPE_NUMBER), col("e", schema.TYPE_NUMBER)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			got := tableMapMismatch(tt.tm, tt.table)
			if tt.want == "" {
				is.Equal(got, "")
				return
			}
			if !strings.Contains(got, tt.want) {
				t.Fatalf("mismatch %q does not contain %q", got, tt.want)
			}
		})
	}
}

// The Table_map event carries the signedness of numeric columns and the
// collation of string columns also under binlog_row_metadata=MINIMAL, which
// tell INT from INT UNSIGNED and VARCHAR from BLOB.
func TestTableMapMismatch_SignednessAndStringKind(t *testing.T) {
	unsignedCol := func(name string, typ int) schema.TableColumn {
		c := col(name, typ)
		c.IsUnsigned = true
		return c
	}
	rawCol := func(name string, typ int, raw string) schema.TableColumn {
		c := col(name, typ)
		c.RawType = raw
		return c
	}
	// types: BIGINT id, INT a, VARCHAR v, INT b. Only numeric columns are in
	// the signedness bitmap, most significant bit first: id, a, b.
	withBitmap := func(bits byte) *replication.TableMapEvent {
		tm := tableMapOf([]byte{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_LONG})
		tm.ColumnCount = 4
		tm.SignednessBitmap = []byte{bits}
		return tm
	}
	// types: id BIGINT, c VARCHAR, d BLOB (TEXT and BLOB share the type).
	withCharsets := func(c, d uint64) *replication.TableMapEvent {
		tm := tableMapOf([]byte{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_BLOB})
		tm.ColumnCount = 3
		tm.ColumnCharset = []uint64{c, d}
		return tm
	}
	idCol := col("id", schema.TYPE_NUMBER)

	tests := []struct {
		name  string
		tm    *replication.TableMapEvent
		table *schema.Table
		want  string
	}{
		{
			name:  "signed in the event and the table",
			tm:    withBitmap(0x00),
			table: tableOf(idCol, col("a", schema.TYPE_NUMBER), col("v", schema.TYPE_STRING), col("b", schema.TYPE_NUMBER)),
		},
		{
			name:  "unsigned in the event and the table",
			tm:    withBitmap(0x40),
			table: tableOf(idCol, unsignedCol("a", schema.TYPE_NUMBER), col("v", schema.TYPE_STRING), col("b", schema.TYPE_NUMBER)),
		},
		{
			name:  "INT to INT UNSIGNED: -5 would be read as 4294967291",
			tm:    withBitmap(0x00),
			table: tableOf(idCol, unsignedCol("a", schema.TYPE_NUMBER), col("v", schema.TYPE_STRING), col("b", schema.TYPE_NUMBER)),
			want:  `column 2 ("a" in the table) is a signed integer in the event and an unsigned integer in the table`,
		},
		{
			name:  "INT UNSIGNED to INT",
			tm:    withBitmap(0x20), // b unsigned: the third numeric column
			table: tableOf(idCol, col("a", schema.TYPE_NUMBER), col("v", schema.TYPE_STRING), col("b", schema.TYPE_NUMBER)),
			want:  `column 4 ("b" in the table) is an unsigned integer in the event and a signed integer in the table`,
		},
		{
			name:  "no signedness in the event: nothing to compare",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_LONG}),
			table: tableOf(unsignedCol("a", schema.TYPE_NUMBER)),
		},
		{
			name:  "text in the event and the table",
			tm:    withCharsets(255, 255),
			table: tableOf(idCol, col("c", schema.TYPE_STRING), rawCol("d", schema.TYPE_STRING, "text")),
		},
		{
			name:  "binary in the event and the table",
			tm:    withCharsets(63, 63),
			table: tableOf(idCol, col("c", schema.TYPE_BINARY), rawCol("d", schema.TYPE_STRING, "longblob")),
		},
		{
			name:  "VARCHAR to BLOB",
			tm:    withCharsets(255, 255),
			table: tableOf(idCol, rawCol("c", schema.TYPE_STRING, "blob"), rawCol("d", schema.TYPE_STRING, "text")),
			want:  `column 2 ("c" in the table) is a text string in the event and a binary string in the table`,
		},
		{
			name:  "BLOB to TEXT",
			tm:    withCharsets(255, 63),
			table: tableOf(idCol, col("c", schema.TYPE_STRING), rawCol("d", schema.TYPE_STRING, "text")),
			want:  `column 3 ("d" in the table) is a binary string in the event and a text string in the table`,
		},
		{
			name:  "VARCHAR to VARBINARY",
			tm:    withCharsets(255, 255),
			table: tableOf(idCol, col("c", schema.TYPE_BINARY), rawCol("d", schema.TYPE_STRING, "text")),
			want:  "a text string in the event and a binary string in the table",
		},
		{
			name:  "another text collation is not a different kind",
			tm:    withCharsets(8, 255),
			table: tableOf(idCol, col("c", schema.TYPE_STRING), rawCol("d", schema.TYPE_STRING, "text")),
		},
		{
			name:  "no collations in the event: nothing to compare",
			tm:    tableMapOf([]byte{mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_VARCHAR}),
			table: tableOf(idCol, rawCol("c", schema.TYPE_STRING, "blob")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			got := tableMapMismatch(tt.tm, tt.table)
			if tt.want == "" {
				is.Equal(got, "")
				return
			}
			if !strings.Contains(got, tt.want) {
				t.Fatalf("mismatch %q does not contain %q", got, tt.want)
			}
		})
	}
}
