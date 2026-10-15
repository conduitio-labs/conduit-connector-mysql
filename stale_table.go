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
	"fmt"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

// refreshStaleTable re-reads the table's columns once when a row image does
// not have one value per cached column, and uses them when they fit.
//
// canal caches each table's columns and refreshes the cache only for DDL its
// SQL parser understands. A DDL it cannot parse (for example ADD COLUMN ...
// INVISIBLE) is skipped with the stale cache kept, so every later event of
// the table looks one column too wide, although the table's current columns
// match it. Re-reading them resolves that case without a halt.
//
// Invariant 6: the right number of values does not make the re-read columns
// the ones the row was written under. A later DDL can keep the width (DROP
// COLUMN b, ADD COLUMN e), and mapping the values onto those columns would
// put every value after the dropped one into the wrong column, silently. So
// the re-read columns are used only if they also match the event's Table_map
// (tableMapMismatch). It returns the mismatch when they do not, for OnRow to
// halt with; an event that does not fit even after the re-read is left for
// buildPayload to halt on.
//
// canal has already converted the values of the cached table's unsigned
// columns (RowsEvent.handleUnsigned) by index. If the cached table had any,
// those conversions may have hit other columns, so the fresh columns are not
// used and the event halts; the cache is fresh by then, so a restart reads the
// event again correctly (and checkTableMap still compares it with the event).
func (h *cdcEventHandler) refreshStaleTable(ctx context.Context, e *canal.RowsEvent) (mismatch string) {
	if h.canal == nil || e.Table == nil || rowsFit(e.Rows, len(e.Table.Columns)) {
		return ""
	}
	stale := e.Table
	h.canal.ClearTableCache([]byte(stale.Schema), []byte(stale.Name))
	fresh, err := h.canal.GetTable(stale.Schema, stale.Name)
	if err != nil {
		sdk.Logger(ctx).Warn().Err(err).Str("table", stale.Schema+"."+stale.Name).
			Msg("could not re-read the table's columns after a row image did not match them")
		return ""
	}
	if !rowsFit(e.Rows, len(fresh.Columns)) {
		return "" // written under another schema: buildPayload halts
	}
	if e.TableMap == nil {
		return "" // nothing to check the re-read columns against: do not use them
	}
	if mismatch := tableMapMismatch(e.TableMap, fresh); mismatch != "" {
		sdk.Logger(ctx).Warn().Str("table", stale.Schema+"."+stale.Name).Str("mismatch", mismatch).
			Msg("the re-read columns of the table have the right count but are not the ones the row was written " +
				"under; not using them")
		return mismatch
	}
	if len(stale.UnsignedColumns) > 0 {
		sdk.Logger(ctx).Warn().Str("table", stale.Schema+"."+stale.Name).
			Msg("the cached columns of the table were stale (a DDL canal could not parse); its unsigned columns may " +
				"have been converted at the wrong positions, so this event halts; a restart reads it again with the " +
				"current columns")
		return ""
	}
	sdk.Logger(ctx).Info().Str("table", stale.Schema+"."+stale.Name).
		Msg("re-read the table's columns: the cached ones were stale (a DDL canal could not parse)")
	e.Table = fresh
	convertUnsigned(fresh, e.Rows)
	return ""
}

// checkTableMap halts when the columns the event will be mapped onto are not
// the ones it was written under, even though their number fits.
//
// Invariant 6: the connector decodes every event with the table's columns as
// they are when it reads the event. A DROP COLUMN plus an ADD COLUMN that ran
// after the event was written (while the connector was stopped or lagging)
// keeps the column count, so the width check passes while every value after
// the dropped column lands in the wrong column. The Table_map event the row
// was written under has the column types, and with binlog_row_metadata=FULL
// the names; see tableMapMismatch for what that can and cannot tell apart.
// refreshMismatch is what refreshStaleTable found, if it ran.
func (h *cdcEventHandler) checkTableMap(e *canal.RowsEvent, refreshMismatch string) error {
	mismatch := refreshMismatch
	if mismatch == "" && e.Table != nil && rowsFit(e.Rows, len(e.Table.Columns)) {
		mismatch = tableMapMismatch(e.TableMap, e.Table)
	}
	if mismatch == "" {
		return nil
	}
	var logPos uint32
	if e.Header != nil {
		logPos = e.Header.LogPos
	}
	return newError(ErrRowColumnsMismatch, CodeRowColumnsMismatch, "tables",
		"The event was written under other columns than the table has now (a column was dropped and another added, "+
			"or changed type, after it was written), so its values cannot be mapped to columns, and restarting does "+
			"not help: it halts the same way on every restart. Reset the pipeline's position to snapshot again "+
			"(expect duplicates; see the README on recovery). To compare column names as well as types, set "+
			"binlog_row_metadata=FULL on the server (MySQL 8.0.1+, MariaDB 10.5+); it applies to events written after it is set",
		"a binlog event for table %s.%s in %s (event end offset %d) was written under other columns than the table "+
			"has now: %s",
		e.Table.Schema, e.Table.Name, h.canal.SyncedPosition().Name, logPos, mismatch)
}

// tableMapMismatch compares the columns a row image was written under (the
// Table_map event) with the columns of table, and returns the first
// difference, or "" if they are compatible or the event has no Table_map.
//
// What is compared:
//   - the column count;
//   - each column's type, by family (integers, floats, decimals, enum, set,
//     strings and binary, and each temporal type). A family is as precise as
//     canal's own column types allow, and coarse enough that widening a
//     column (INT to BIGINT, VARCHAR(10) to VARCHAR(100)) while the connector
//     is stopped does not halt it. A type the table map has and this does not
//     know compares equal.
//   - each column's name, case-insensitively, when the event has them, that
//     is when the server logs binlog_row_metadata=FULL. A rename while the
//     connector is stopped is then indistinguishable from a drop plus an add,
//     so it halts too.
//
// Without names, a column dropped and another of the same family added at the
// same position cannot be told apart from the original, and the values are
// mapped onto it. Only binlog_row_metadata=FULL closes that.
func tableMapMismatch(tm *replication.TableMapEvent, table *schema.Table) string {
	if tm == nil || table == nil {
		return ""
	}
	if len(tm.ColumnType) != len(table.Columns) {
		return fmt.Sprintf("the event has %d columns, the table has %d", len(tm.ColumnType), len(table.Columns))
	}
	var names []string
	if len(tm.ColumnName) == len(tm.ColumnType) {
		names = tm.ColumnNameString()
	}
	for i, col := range table.Columns {
		if names != nil && !strings.EqualFold(names[i], col.Name) {
			return fmt.Sprintf("column %d is %q in the event and %q in the table", i+1, names[i], col.Name)
		}
		have, want := binlogKind(tm, i), canalKind(col.Type)
		if have != kindUnknown && want != kindUnknown && have != want {
			return fmt.Sprintf("column %d (%q in the table) is %s in the event and %s in the table", i+1, col.Name, have, want)
		}
	}
	return ""
}

// columnKind is a family of column types that canal's schema and the binlog's
// Table_map event can both express.
type columnKind string

const (
	kindUnknown   columnKind = ""
	kindInteger   columnKind = "an integer"
	kindFloat     columnKind = "a float"
	kindDecimal   columnKind = "a decimal"
	kindEnum      columnKind = "an enum"
	kindSet       columnKind = "a set"
	kindBytes     columnKind = "a string or binary"
	kindDatetime  columnKind = "a datetime"
	kindTimestamp columnKind = "a timestamp"
	kindDate      columnKind = "a date"
	kindTime      columnKind = "a time"
	kindBit       columnKind = "a bit"
	kindJSON      columnKind = "json"
)

// binlogKind is the family of column i in a Table_map event.
func binlogKind(tm *replication.TableMapEvent, i int) columnKind {
	t := tm.ColumnType[i]
	if t == mysql.MYSQL_TYPE_STRING && i < len(tm.ColumnMeta) {
		// ENUM and SET are logged as STRING, with the real type in the metadata.
		switch byte(tm.ColumnMeta[i] >> 8) {
		case mysql.MYSQL_TYPE_ENUM:
			return kindEnum
		case mysql.MYSQL_TYPE_SET:
			return kindSet
		}
	}
	switch t {
	case mysql.MYSQL_TYPE_TINY, mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG,
		mysql.MYSQL_TYPE_LONGLONG, mysql.MYSQL_TYPE_YEAR:
		return kindInteger
	case mysql.MYSQL_TYPE_FLOAT, mysql.MYSQL_TYPE_DOUBLE:
		return kindFloat
	case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL:
		return kindDecimal
	case mysql.MYSQL_TYPE_ENUM:
		return kindEnum
	case mysql.MYSQL_TYPE_SET:
		return kindSet
	case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR, mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_BLOB,
		mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_GEOMETRY:
		return kindBytes
	case mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_DATETIME2:
		return kindDatetime
	case mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIMESTAMP2:
		return kindTimestamp
	case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE:
		return kindDate
	case mysql.MYSQL_TYPE_TIME, mysql.MYSQL_TYPE_TIME2:
		return kindTime
	case mysql.MYSQL_TYPE_BIT:
		return kindBit
	case mysql.MYSQL_TYPE_JSON:
		return kindJSON
	}
	return kindUnknown
}

// canalKind is the family of a canal schema column type.
func canalKind(t int) columnKind {
	switch t {
	case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
		return kindInteger
	case schema.TYPE_FLOAT:
		return kindFloat
	case schema.TYPE_DECIMAL:
		return kindDecimal
	case schema.TYPE_ENUM:
		return kindEnum
	case schema.TYPE_SET:
		return kindSet
	case schema.TYPE_STRING, schema.TYPE_BINARY, schema.TYPE_POINT:
		return kindBytes
	case schema.TYPE_DATETIME:
		return kindDatetime
	case schema.TYPE_TIMESTAMP:
		return kindTimestamp
	case schema.TYPE_DATE:
		return kindDate
	case schema.TYPE_TIME:
		return kindTime
	case schema.TYPE_BIT:
		return kindBit
	case schema.TYPE_JSON:
		return kindJSON
	}
	return kindUnknown
}

// rowsFit reports whether every row has one value per column.
func rowsFit(rows [][]any, columns int) bool {
	for _, r := range rows {
		if len(r) != columns {
			return false
		}
	}
	return true
}

// convertUnsigned is canal's RowsEvent.handleUnsigned (go-mysql v1.14.0,
// canal/rows.go, MIT), which is unexported: the binlog does not say whether
// an integer is unsigned, so values of unsigned columns are converted after
// decoding, by the table's column indexes.
//
// The conversions reinterpret the bits on purpose: a negative value in a
// signed integer is the binlog's encoding of a large unsigned one.
//
//nolint:gosec // G115: the reinterpretation is the point, as in canal.
func convertUnsigned(table *schema.Table, rows [][]any) {
	const maxMediumintUnsigned int32 = 16777215
	for _, row := range rows {
		for _, idx := range table.UnsignedColumns {
			if idx >= len(row) {
				continue
			}
			switch v := row[idx].(type) {
			case int8:
				row[idx] = uint8(v)
			case int16:
				row[idx] = uint16(v)
			case int32:
				if v < 0 && table.Columns[idx].Type == schema.TYPE_MEDIUM_INT {
					row[idx] = uint32(maxMediumintUnsigned + v + 1)
				} else {
					row[idx] = uint32(v)
				}
			case int64:
				row[idx] = uint64(v)
			case int:
				row[idx] = uint(v)
			}
		}
	}
}
