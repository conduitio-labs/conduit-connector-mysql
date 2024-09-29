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

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Source struct {
	sdk.UnimplementedSource

	config        common.SourceConfig
	configFromDsn *mysql.Config

	db *sqlx.DB

	iterator common.Iterator
}

func NewSource() sdk.Source {
	// Create Source and wrap it in the default middleware.
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() config.Parameters {
	// Parameters is a map of named Parameters that describe how to configure
	// the Source. Parameters can be generated from SourceConfig with paramgen.
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg config.Config) (err error) {
	if err := sdk.Util.ParseConfig(ctx, cfg, &s.config, s.config.Parameters()); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	s.configFromDsn, err = mysql.ParseDSN(s.config.URL)
	if err != nil {
		return fmt.Errorf("failed to parse given URL: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("configured source connector")
	return nil
}

func (s *Source) Open(ctx context.Context, sdkPos opencdc.Position) (err error) {
	s.db, err = sqlx.Open("mysql", s.config.URL)
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("Detecting all tables...")
	s.config.Tables, err = s.getAndFilterTables(ctx, s.db, s.configFromDsn.DBName)
	if err != nil {
		return err
	}

	sdk.Logger(ctx).Info().
		Strs("tables", s.config.Tables).
		Int("count", len(s.config.Tables)).
		Msgf("Successfully detected tables")

	tableKeys, err := getTableKeys(s.db, s.configFromDsn.DBName, s.config.Tables)
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
		tableKeys:             tableKeys,
		startSnapshotPosition: pos.SnapshotPosition,
		startCdcPosition:      pos.CdcPosition,
		database:              s.configFromDsn.DBName,
		tables:                s.config.Tables,
		serverID:              serverID,
		mysqlConfig:           s.configFromDsn,
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
	return s.iterator.Next(ctx)
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

func (s *Source) getAndFilterTables(ctx context.Context, db *sqlx.DB, database string) ([]string, error) {
	query := fmt.Sprintf("SELECT table_name	FROM information_schema.tables	WHERE table_schema = '%s'", database)

	rows, err := db.Queryx(query)

	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	includedTables := make(map[string]bool)

	// Iterate through all the rules
	for _, rule := range s.config.Tables {
		action, regexPattern, err := parseRule(rule)
		if err != nil {
			return nil, err
		}

		if regexPattern == common.AllTablesWildcard {
			for _, table := range tables {
				includedTables[table] = true
			}
		} else {

			// Compile the regex
			re, err := regexp.Compile(regexPattern)
			if err != nil {
				return nil, fmt.Errorf("invalid regex pattern: %w", err)
			}

			// Apply the rule to all tables
			for _, table := range tables {
				if re.MatchString(table) {
					if action == Include {
						includedTables[table] = true // Include this table
					} else if action == Exclude {
						includedTables[table] = false // Exclude this table
					}
				}
			}
		}
	}
	var result []string
	// Collect all the tables that were included (true in map)
	for table, included := range includedTables {
		if included {
			result = append(result, table)
		}
	}

	return result, nil
}

type Action int

const (
	Include Action = iota
	Exclude
)

// Helper function to parse the rule and return the action and the regex pattern
func parseRule(rule string) (Action, string, error) {
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

// function to get the primary key of a table
func getPrimaryKey(db *sqlx.DB, database, table string) (common.PrimaryKeyName, error) {
	var primaryKey struct {
		ColumnName common.PrimaryKeyName `db:"COLUMN_NAME"`
	}

	row := db.QueryRowx(`
		SELECT COLUMN_NAME
		FROM information_schema.key_column_usage
		WHERE
			constraint_name = 'PRIMARY'
			AND table_schema = ?
			AND table_name = ?
	`, database, table)

	if err := row.StructScan(&primaryKey); err != nil {
		return "", fmt.Errorf("failed to get primary key from table %s: %w", table, err)
	}
	if err := row.Err(); err != nil {
		return "", fmt.Errorf("failed to scan primary key from table %s: %w", table, err)
	}

	return primaryKey.ColumnName, nil
}

func getTableKeys(db *sqlx.DB, database string, tables []string) (common.TableKeys, error) {
	tableKeys := make(common.TableKeys)

	for _, table := range tables {
		primaryKey, err := getPrimaryKey(db, database, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key for table %q: %w", table, err)
		}

		tableKeys[common.TableName(table)] = primaryKey
	}

	return tableKeys, nil
}
