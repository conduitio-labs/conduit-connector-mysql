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
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type Config struct {
	// The connection string for the MySQL database.
	DSN string `json:"dsn" validate:"required"`
}

type SourceConfig struct {
	sdk.DefaultSourceMiddleware

	Config

	// Holds the custom configuration that each table can have.
	TableConfig map[string]TableConfig `json:"tableConfig"`

	// Represents the tables to read from.
	//  - By default, no tables are included, but can be modified by adding a comma-separated string of regex patterns.
	//  - They are applied in the order that they are provided, so the final regex supersedes all previous ones.
	//  - To include all tables, use "*". You can then filter that list by adding a comma-separated string of regex patterns.
	//  - To set an "include" regex, add "+" or nothing in front of the regex.
	//  - To set an "exclude" regex, add "-" in front of the regex.
	//  - e.g. "-.*meta$, wp_postmeta" will exclude all tables ending with "meta" but include the table "wp_postmeta".
	Tables []string `json:"tables" validate:"required"`

	// Same as Tables, but it applies to the snapshot process.
	// When defined, it overrides the Tables parameter for snapshotting.
	SnapshotTables []string `json:"snapshot.tables"`

	// Same as Tables, but it applies to the Change Data Capture (CDC) process.
	// When defined, it overrides the Tables parameter for CDC.
	CDCTables []string `json:"cdc.tables"`

	// Disables verbose cdc driver logs.
	DisableLogs bool `json:"cdc.disableLogs"`

	// Limits how many rows should be retrieved on each database fetch on snapshot mode.
	FetchSize uint64 `json:"snapshot.fetchSize" default:"10000"`

	// Allows a snapshot of a table with neither a primary key
	// nor a defined sorting column. The opencdc.Position won't record the last record
	// read from a table.
	UnsafeSnapshot bool `json:"snapshot.unsafe"`

	// Controls whether the snapshot is done.
	SnapshotEnabled bool `json:"snapshot.enabled" default:"true"`

	mysqlCfg *mysql.Config
}

func (s *SourceConfig) MysqlCfg() *mysql.Config {
	return s.mysqlCfg
}

func (s *SourceConfig) Validate(context.Context) error {
	mysqlCfg, err := mysql.ParseDSN(s.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	// we need to take control over how do we handle time.Time values
	mysqlCfg.ParseTime = true

	s.mysqlCfg = mysqlCfg

	return nil
}

type TableConfig struct {
	// Allows to force using a custom column to sort the snapshot.
	SortingColumn string `json:"sortingColumn"`
}

const (
	DefaultFetchSize = 50000
	// AllTablesWildcard can be used if you'd like to listen to all tables.
	AllTablesWildcard = "*"
)

type Source struct {
	sdk.UnimplementedSource

	config   SourceConfig
	db       *sqlx.DB
	iterator common.Iterator
}

var (
	defaultBatchDelay = time.Second
	defaultBatchSize  = 10000
)

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		config: SourceConfig{
			DefaultSourceMiddleware: sdk.DefaultSourceMiddleware{
				SourceWithBatch: sdk.SourceWithBatch{
					BatchSize:  &defaultBatchSize,
					BatchDelay: &defaultBatchDelay,
				},
			},
		},
	})
}

func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

func (s *Source) Open(ctx context.Context, sdkPos opencdc.Position) (err error) {
	s.db, err = sqlx.Open("mysql", s.config.MysqlCfg().FormatDSN())
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	dbName := s.config.MysqlCfg().DBName

	tableKeys, err := s.getTableKeysFromTables(ctx, dbName)
	if err != nil {
		return fmt.Errorf("failed to get table keys: %w", err)
	}
	filtered, err := s.filterTables(ctx, dbName, tableKeys)
	if err != nil {
		return fmt.Errorf("failed to filter table keys: %w", err)
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
		tableKeys:             filtered,
		startSnapshotPosition: pos.SnapshotPosition,
		startCdcPosition:      pos.CdcPosition,
		database:              dbName,
		serverID:              serverID,
		mysqlConfig:           s.config.MysqlCfg(),
		disableCanalLogging:   s.config.DisableLogs,
		fetchSize:             s.config.FetchSize,
		snapshotEnabled:       s.config.SnapshotEnabled,
	})
	if err != nil {
		return fmt.Errorf("failed to create snapshot iterator: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("opened source connector")

	return nil
}

func (s *Source) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	//nolint:wrapcheck // error already wrapped in iterator
	return s.iterator.ReadN(ctx, n)
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

func (s *Source) getTableKeysFromTables(
	ctx context.Context,
	dbName string,
) (common.TableKeys, error) {
	keys := make(common.TableKeys)

	var tables []string
	err := s.db.SelectContext(ctx,
		&tables,
		"SELECT table_name FROM information_schema.tables WHERE table_schema = ?",
		dbName,
	)
	if err != nil {
		return keys, fmt.Errorf("failed to fetch table names from database")
	}

	tableKeys := make(common.TableKeys)

	for _, table := range tables {
		preconfiguredTableKey, ok := s.config.TableConfig[table]
		if ok {
			tableKeys[table] = common.PrimaryKeys{preconfiguredTableKey.SortingColumn}
			continue
		}

		primaryKeys, err := common.GetPrimaryKeys(s.db, dbName, table)
		if err != nil {
			if s.config.UnsafeSnapshot && errors.Is(err, sql.ErrNoRows) {
				sdk.Logger(ctx).Warn().Msgf(
					"table %s has no primary key, doing an unsafe snapshot ", table)

				// The snapshot iterator should be able to interpret a zero
				// value table key as a table where we cannot do a sorted
				// snapshot.

				tableKeys[table] = common.PrimaryKeys{}
				continue
			}

			return tableKeys, fmt.Errorf(
				"failed to get primary key for table %s. You might want to add a `tableConfig.<table name>.sortingColumn entry, or enable `unsafeSnapshot` mode: %w",
				table, err)
		}

		tableKeys[table] = primaryKeys
	}

	return tableKeys, nil
}

// createCanalRegexes creates regex patterns for Canal from the filtered table names.
func createCanalRegexes(database string, tables []string) []string {
	canalRegexes := make([]string, 0, len(tables))
	for _, table := range tables {
		// prefix with db name because Canal does the same for the key, so we can't prefix with ^ to prevent undesired matches.
		// Append $ to prevent undesired matches.
		canalRegexes = append(canalRegexes, fmt.Sprintf("^%s.%s$", database, regexp.QuoteMeta(table)))
	}
	return canalRegexes
}

func (s *Source) processTableRule(rule string, tables []string, includedTables map[string]bool) error {
	// trim leading and trailing spaces from rule
	rule = strings.TrimSpace(rule)

	if rule == AllTablesWildcard {
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

type filteredTableKeys struct {
	Snapshot common.TableKeys
	Cdc      CdcTableKeys
}

type CdcTableKeys struct {
	TableRegexes []string
	TableKeys    common.TableKeys
}

func (s *Source) filterTables(
	ctx context.Context,
	dbName string,
	tableKeys common.TableKeys,
) (filteredTableKeys, error) {
	filtered := filteredTableKeys{
		Snapshot: common.TableKeys{},
		Cdc: CdcTableKeys{
			TableKeys:    common.TableKeys{},
			TableRegexes: []string{},
		},
	}

	if len(s.config.SnapshotTables) != 0 {
		for _, rule := range s.config.SnapshotTables {
			action, regexPattern, err := ParseRule(rule)
			if err != nil {
				return filtered, err
			}

			re, err := regexp.Compile(regexPattern)
			if err != nil {
				return filtered, fmt.Errorf("invalid regex pattern: %w", err)
			}

			for table, keys := range tableKeys {
				if rule == AllTablesWildcard {
					filtered.Snapshot[table] = keys
					continue
				}

				if re.MatchString(table) && action == Include {
					filtered.Snapshot[table] = keys
				}
			}
		}
	} else {
		for _, rule := range s.config.Tables {
			action, regexPattern, err := ParseRule(rule)
			if err != nil {
				return filtered, err
			}

			re, err := regexp.Compile(regexPattern)
			if err != nil {
				return filtered, fmt.Errorf("invalid regex pattern: %w", err)
			}

			for table, keys := range tableKeys {
				if rule == AllTablesWildcard {
					filtered.Snapshot[table] = keys
					continue
				}

				if re.MatchString(table) && action == Include {
					filtered.Snapshot[table] = keys
				}
			}
		}
	}

	if len(s.config.CDCTables) != 0 {
		for _, rule := range s.config.CDCTables {
			action, regexPattern, err := ParseRule(rule)
			if err != nil {
				return filtered, err
			}

			re, err := regexp.Compile(regexPattern)
			if err != nil {
				return filtered, fmt.Errorf("invalid regex pattern: %w", err)
			}

			for table, keys := range tableKeys {
				if rule == AllTablesWildcard {
					filtered.Cdc.TableKeys[table] = keys
					continue
				}

				if re.MatchString(table) && action == Include {
					filtered.Cdc.TableKeys[table] = keys
				}
			}
		}
	} else {
		for _, rule := range s.config.Tables {
			action, regexPattern, err := ParseRule(rule)
			if err != nil {
				return filtered, err
			}

			re, err := regexp.Compile(regexPattern)
			if err != nil {
				return filtered, fmt.Errorf("invalid regex pattern: %w", err)
			}

			for table, keys := range tableKeys {
				if rule == AllTablesWildcard {
					filtered.Cdc.TableKeys[table] = keys
					continue
				}

				if re.MatchString(table) && action == Include {
					filtered.Cdc.TableKeys[table] = keys
				}
			}
		}
	}

	filtered.Cdc.TableRegexes = createCanalRegexes(dbName, filtered.Cdc.TableKeys.GetTables())

	return filtered, nil
}
