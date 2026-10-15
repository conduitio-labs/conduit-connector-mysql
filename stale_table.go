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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

// refreshStaleTable re-reads the table's columns once when a row image does
// not have one value per cached column, and uses them when they fit.
//
// canal caches each table's columns and refreshes the cache only for DDL its
// SQL parser understands. A DDL it cannot parse (for example ADD COLUMN ...
// INVISIBLE) is skipped with the stale cache kept, so every later event of
// the table looks one column too wide, although the table's current columns
// match it. Re-reading them resolves that case without a halt. An event that
// really was written under another schema still does not fit after the
// re-read, and buildPayload halts on it (invariant 6).
//
// canal has already converted the values of the cached table's unsigned
// columns (RowsEvent.handleUnsigned) by index. If the cached table had any,
// those conversions may have hit other columns, so the fresh columns are not
// used and the event halts; the cache is fresh by then, so a restart reads the
// event again correctly.
func (h *cdcEventHandler) refreshStaleTable(ctx context.Context, e *canal.RowsEvent) {
	if h.canal == nil || e.Table == nil || rowsFit(e.Rows, len(e.Table.Columns)) {
		return
	}
	stale := e.Table
	h.canal.ClearTableCache([]byte(stale.Schema), []byte(stale.Name))
	fresh, err := h.canal.GetTable(stale.Schema, stale.Name)
	if err != nil {
		sdk.Logger(ctx).Warn().Err(err).Str("table", stale.Schema+"."+stale.Name).
			Msg("could not re-read the table's columns after a row image did not match them")
		return
	}
	if !rowsFit(e.Rows, len(fresh.Columns)) {
		return // written under another schema: buildPayload halts
	}
	if len(stale.UnsignedColumns) > 0 {
		sdk.Logger(ctx).Warn().Str("table", stale.Schema+"."+stale.Name).
			Msg("the cached columns of the table were stale (a DDL canal could not parse); its unsigned columns may " +
				"have been converted at the wrong positions, so this event halts; a restart reads it again with the " +
				"current columns")
		return
	}
	sdk.Logger(ctx).Info().Str("table", stale.Schema+"."+stale.Name).
		Msg("re-read the table's columns: the cached ones were stale (a DDL canal could not parse)")
	e.Table = fresh
	convertUnsigned(fresh, e.Rows)
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
