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
	"fmt"
	"regexp"
	"strings"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Source struct {
	sdk.UnimplementedSource

	config common.SourceConfig

	db *sqlx.DB

	iterator common.Iterator
}

func NewSource() sdk.Source {
	// Create Source and wrap it in the default middleware.
	return sdk.SourceWithMiddleware(&Source{})
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

func (s *Source) Open(ctx context.Context, sdkPos opencdc.Position) (err error) {
	mysqlCfg, err := mysql.ParseDSN(s.config.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse given URL: %w", err)
	}

	// force parse time to true, as we need to take control over how do we
	// handle time.Time values
	mysqlCfg.ParseTime = true

	s.db, err = sqlx.Open("mysql", mysqlCfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	var canalRegexes []string
	sdk.Logger(ctx).Info().Msg("Parsing table regexes...")
	s.config.Tables, canalRegexes, err = s.getAndFilterTables(ctx, s.db, mysqlCfg.DBName)
	if err != nil {
		return err
	}

	sdk.Logger(ctx).Info().
		Strs("tables", s.config.Tables).
		Int("count", len(s.config.Tables)).
		Msgf("Successfully detected tables")

	tableKeys, err := s.getTableKeys(ctx, mysqlCfg.DBName)
	if err != nil {
		return fmt.Errorf("failed to get table keys: %w", err)
	}

	serverID, err := common.GetServerID(ctx, s.db)
	if err != nil {
		return fmt.Errorf("failed to get server id: %w", err)
	}

	// set positions by default to nil, so that iterators know if starting from no position
	var pos common.Position
	if sdkPos != nil {
		parsed, err := common.ParseSDKPosition(sdkPos)
		if err != nil {
			return fmt.Errorf("bad source position given: %w", err)
		}
		pos = parsed
	}

	s.iterator, err = newCombinedIterator(ctx, combinedIteratorConfig{
		db:                    s.db,
		tableSortCols:         tableKeys,
		startSnapshotPosition: pos.SnapshotPosition,
		startCdcPosition:      pos.CdcPosition,
		database:              mysqlCfg.DBName,
		tables:                s.config.Tables,
		canalRegexes:          canalRegexes,
		serverID:              serverID,
		mysqlConfig:           mysqlCfg,
		disableCanalLogging:   s.config.DisableCanalLogs,
		fetchSize:             s.config.FetchSize,
	})
	if err != nil {
		return fmt.Errorf("failed to create snapshot iterator: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("opened source connector")

	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	//nolint:wrapcheck // error already wrapped in iterator
	return s.iterator.Read(ctx)
}

func (s *Source) Ack(ctx context.Context, _ opencdc.Position) error {
	//nolint:wrapcheck // error already wrapped in iterator
	return s.iterator.Ack(ctx, opencdc.Position{})
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		if err := s.iterator.Teardown(ctx); err != nil {
			//nolint:wrapcheck // error already wrapped in iterator
			return err
		}
	}

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}

	return nil
}

func (s *Source) getAndFilterTables(ctx context.Context, db *sqlx.DB, database string) ([]string, []string, error) {
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = ?"

	rows, err := db.Queryx(query, database) //nolint:sqlclosecheck //false positive in latest version. Issue #35
	if err != nil {
		sdk.Logger(ctx).Error().Err(err).Msg("failed to query tables")
		return nil, nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("rows error: %w", err)
	}

	includedTables := make(map[string]bool)

	// Process each rule
	for _, rule := range s.config.Tables {
		if err := s.processTableRule(rule, tables, includedTables); err != nil {
			return nil, nil, err
		}
	}

	var finalTables []string
	// Collect all the tables that were included (true in map)
	for table, included := range includedTables {
		if included {
			finalTables = append(finalTables, table)
		}
	}

	var canalRegexes []string
	for _, table := range finalTables {
		// prefix with db name because Canal does the same for the key, so we can't prefix with ^ to prevent undesired matches.
		// Append $ to prevent undesired matches.
		canalRegexes = append(canalRegexes, fmt.Sprintf("%s.%s$", database, regexp.QuoteMeta(table)))
	}

	return finalTables, canalRegexes, nil
}

func (s *Source) processTableRule(rule string, tables []string, includedTables map[string]bool) error {
	// trim leading and trailing spaces from rule
	rule = strings.TrimSpace(rule)

	if rule == common.AllTablesWildcard {
		for _, table := range tables {
			includedTables[table] = true
		}
		return nil
	}

	action, regexPattern, err := ParseRule(rule)
	if err != nil {
		return err
	}

	// Compile the regex
	re, err := regexp.Compile(regexPattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %w", err)
	}

	// Apply the rule to all tables
	for _, table := range tables {
		if re.MatchString(table) {
			includedTables[table] = (action == Include)
		}
	}

	return nil
}

type Action int

const (
	Include Action = iota
	Exclude
)

// ParseRule parses a table filter rule and returns the action and pattern.
func ParseRule(rule string) (Action, string, error) {
	if len(rule) < 2 {
		return Include, "", fmt.Errorf("invalid rule format: %s", rule)
	}

	var action Action
	switch rule[0] {
	case '+':
		action = Include
		rule = rule[1:] // Strip the prefix
	case '-':
		action = Exclude
		rule = rule[1:] // Strip the prefix
	default:
		// No prefix means default to Include
		action = Include
	}

	return action, rule, nil // Return action and regex (without the action prefix)
}

// function to get the primary key of a table.
func getPrimaryKey(db *sqlx.DB, database, table string) (string, error) {
	var primaryKey struct {
		ColumnName string `db:"COLUMN_NAME"`
	}

	row := db.QueryRowx(`
		SELECT COLUMN_NAME
		FROM information_schema.key_column_usage
		WHERE constraint_name = 'PRIMARY'
			AND table_schema = ?
			AND table_name = ?
		ORDER BY ORDINAL_POSITION DESC
	`, database, table)

	if err := row.StructScan(&primaryKey); err != nil {
		return "", fmt.Errorf("failed to get primary key from table %s: %w", table, err)
	}
	if err := row.Err(); err != nil {
		return "", fmt.Errorf("failed to scan primary key from table %s: %w", table, err)
	}

	return primaryKey.ColumnName, nil
}

func (s *Source) getTableKeys(ctx context.Context, dbName string) (map[string]string, error) {
	tableKeys := make(map[string]string)

	for _, table := range s.config.Tables {
		preconfiguredTableKey, ok := s.config.TableConfig[table]
		if ok {
			tableKeys[table] = preconfiguredTableKey.SortingColumn
			continue
		}

		primaryKey, err := getPrimaryKey(s.db, dbName, table)
		if err != nil {
			if s.config.UnsafeSnapshot {
				sdk.Logger(ctx).Warn().Msgf(
					"table %s has no primary key, doing an unsafe snapshot ", table)

				// The snapshot iterator should be able to interpret a zero
				// value table key as a table where we cannot do a sorted
				// snapshot.

				tableKeys[table] = ""
				continue
			}

			return nil, fmt.Errorf(
				"failed to get primary key for table %s. You might want to add a `tableConfig.<table name>.sortingColumn entry, or enable `unsafeSnapshot` mode: %w",
				table, err)
		}

		tableKeys[table] = primaryKey
	}

	return tableKeys, nil
}
