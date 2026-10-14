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
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// ErrSortingColumnNotUnique is returned by Open when a tableConfig sortingColumn
// cannot page the snapshot without losing rows. Use errors.Is to match; the
// message names the table, the config path and the fix.
var ErrSortingColumnNotUnique = errors.New("sorting column must be unique and not null")

func sortingColumnPath(table string) string {
	return "tableConfig." + table + ".sortingColumn"
}

// validateSortingColumn checks that column can order table's snapshot.
//
// Invariant 3: the snapshot reads chunks with "col > last_read ORDER BY col
// LIMIT n" and records last_read in the position. With duplicate values, rows
// that share the value at a chunk boundary and did not fit in the chunk are
// skipped for good. MIN/MAX and ">" ignore NULL, so NULL rows are never read at
// all, even under a unique index (MySQL allows many NULLs in one). Both are
// silent data loss, so the column is required to be NOT NULL and the only column
// of a primary key or unique index.
func validateSortingColumn(db *sqlx.DB, database, table, column string) error {
	var nullable string
	err := db.Get(&nullable, `
		SELECT IS_NULLABLE
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_name = ?
	`, database, table, column)
	if errors.Is(err, sql.ErrNoRows) {
		return newError(ErrSortingColumnNotUnique, CodeSortingColumnInvalid, sortingColumnPath(table),
			"set it to a NOT NULL, unique column of the table, or add a primary key, or exclude the table in tables",
			"table %q has no column %q", table, column)
	} else if err != nil {
		return fmt.Errorf("failed to inspect sorting column %q of table %q: %w", column, table, err)
	}

	if !strings.EqualFold(nullable, "NO") {
		return newError(ErrSortingColumnNotUnique, CodeSortingColumnInvalid, sortingColumnPath(table),
			"declare the column NOT NULL or choose another column, add a primary key, or exclude the table in tables",
			"sorting column %q of table %q is nullable, and rows with NULL would be skipped", column, table)
	}

	var indexed []struct {
		IndexName  string         `db:"INDEX_NAME"`
		ColumnName sql.NullString `db:"COLUMN_NAME"`
	}
	err = db.Select(&indexed, `
		SELECT INDEX_NAME, COLUMN_NAME
		FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND non_unique = 0
	`, database, table)
	if err != nil {
		return fmt.Errorf("failed to inspect unique indexes of table %q: %w", table, err)
	}

	columnsPerIndex := make(map[string][]string)
	for _, idx := range indexed {
		columnsPerIndex[idx.IndexName] = append(columnsPerIndex[idx.IndexName], idx.ColumnName.String)
	}
	for _, cols := range columnsPerIndex {
		if len(cols) == 1 && strings.EqualFold(cols[0], column) {
			return nil
		}
	}

	return newError(ErrSortingColumnNotUnique, CodeSortingColumnInvalid, sortingColumnPath(table),
		"add a unique index on the column, choose another column, add a primary key, or exclude the table in tables",
		"sorting column %q of table %q is not the only column of a primary key or unique index, "+
			"and rows sharing a value at a chunk boundary would be skipped", column, table)
}

// validateSortingColumns validates every configured sorting column of the
// tables being read.
func (s *Source) validateSortingColumns() error {
	for _, table := range s.config.Tables {
		cfg, ok := s.config.TableConfig[table]
		if !ok || cfg.SortingColumn == "" {
			continue
		}
		if err := validateSortingColumn(s.db, s.config.MysqlCfg().DBName, table, cfg.SortingColumn); err != nil {
			return err
		}
	}

	return nil
}
