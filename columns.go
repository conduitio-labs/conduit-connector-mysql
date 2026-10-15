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
	"slices"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx"
)

// columnInfo is what information_schema says about a column. Strict mode
// takes nullability and the exact column type from here for the snapshot and
// for change capture alike, so both build the same schema: canal's table
// schema has no nullability, and the driver's column types differ from it.
type columnInfo struct {
	nullable bool
	// dataType is DATA_TYPE, lowercase ("decimal", "bigint").
	dataType string
	// columnType is COLUMN_TYPE, lowercase ("decimal(30,10)", "bigint unsigned").
	columnType string
}

func (c columnInfo) unsigned() bool { return strings.Contains(c.columnType, "unsigned") }

// information_schema DATA_TYPE values strict mode maps differently.
const (
	dataTypeDecimal = "decimal"
	dataTypeNumeric = "numeric"
	dataTypeBigint  = "bigint"
)

func (c columnInfo) isDecimal() bool {
	return c.dataType == dataTypeDecimal || c.dataType == dataTypeNumeric
}

func (c columnInfo) isUnsignedBigint() bool { return c.dataType == dataTypeBigint && c.unsigned() }

// columnCatalog caches columnInfo per table. A table is read again when a
// column is missing (a DDL since the last read); a column that is still
// missing is an error, never "assume NOT NULL".
type columnCatalog struct {
	db       *sqlx.DB
	database string

	mu     sync.Mutex
	tables map[string]map[string]columnInfo
}

func newColumnCatalog(db *sqlx.DB, database string) *columnCatalog {
	return &columnCatalog{db: db, database: database, tables: map[string]map[string]columnInfo{}}
}

// load reads the given tables.
func (c *columnCatalog) load(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	query, args, err := sqlx.In(`SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE
		FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN (?)`, c.database, tables)
	if err != nil {
		return fmt.Errorf("build column query: %w", err)
	}
	rows, err := c.db.QueryxContext(ctx, c.db.Rebind(query), args...)
	if err != nil {
		return newError(ErrNullabilityUnavailable, CodeNullabilityUnavailable, "tables",
			"grant SELECT on the captured tables to the connector's user",
			"could not read information_schema.columns for %v: %v", tables, err)
	}
	defer rows.Close()

	read := map[string]map[string]columnInfo{}
	for rows.Next() {
		var table, column, nullable, dataType, columnType string
		if err := rows.Scan(&table, &column, &nullable, &dataType, &columnType); err != nil {
			return fmt.Errorf("scan information_schema.columns: %w", err)
		}
		if read[table] == nil {
			read[table] = map[string]columnInfo{}
		}
		read[table][column] = columnInfo{
			nullable:   strings.EqualFold(nullable, "YES"),
			dataType:   strings.ToLower(dataType),
			columnType: strings.ToLower(columnType),
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read information_schema.columns: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range tables {
		c.tables[t] = read[t] // nil for a table the user cannot see: every lookup then fails
	}
	return nil
}

// lookup returns the info for the columns of a table, reading the table
// again once if any column is missing.
func (c *columnCatalog) lookup(ctx context.Context, table string, columns []string) (map[string]columnInfo, error) {
	got, missing := c.cached(table, columns)
	if missing != "" {
		if err := c.load(ctx, []string{table}); err != nil {
			return nil, err
		}
		got, missing = c.cached(table, columns)
	}
	if missing != "" {
		return nil, newError(ErrNullabilityUnavailable, CodeNullabilityUnavailable, "tables",
			"grant SELECT on the table to the connector's user, or exclude it; if the table was altered, restart",
			"column %s.%s has no row in information_schema.columns, so its nullability is unknown", table, missing)
	}
	return got, nil
}

func (c *columnCatalog) cached(table string, columns []string) (map[string]columnInfo, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cols := c.tables[table]
	out := make(map[string]columnInfo, len(columns))
	for _, col := range columns {
		info, ok := cols[col]
		if !ok {
			return nil, col
		}
		out[col] = info
	}
	return out, ""
}

// snapshot returns a copy of the cache, for the Open-time warnings.
func (c *columnCatalog) snapshot() map[string]map[string]columnInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]map[string]columnInfo, len(c.tables))
	for t, cols := range c.tables {
		out[t] = cols
	}
	return out
}

func sortedColumns(cols map[string]columnInfo) []string {
	names := make([]string, 0, len(cols))
	for n := range cols {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}
