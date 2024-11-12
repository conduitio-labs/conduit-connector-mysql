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

	s.configFromDsn, err = mysql.ParseDSN(s.config.DSN)
	if err != nil {
		return fmt.Errorf("failed to parse given URL: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("configured source connector")
	return nil
}

func (s *Source) Open(ctx context.Context, sdkPos opencdc.Position) (err error) {
	s.db, err = sqlx.Open("mysql", s.config.DSN)
	if err != nil {
		return fmt.Errorf("failed to connect to mysql: %w", err)
	}

	tableKeys, err := s.getTableKeys()
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

func getPrimaryKey(db *sqlx.DB, database, table string) (string, error) {
	var primaryKey struct {
		ColumnName string `db:"COLUMN_NAME"`
	}

	row := db.QueryRowx(`
		SELECT COLUMN_NAME
		FROM information_schema.key_column_usage
		WHERE
			constraint_name = 'PRIMARY'
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

func (s *Source) getTableKeys() (common.TableSortColumns, error) {
	tableKeys := make(common.TableSortColumns)

	for _, table := range s.config.Tables {
		preconfiguredTableKey, ok := s.config.TableKeys[table]
		if ok {
			tableKeys[table] = preconfiguredTableKey.SortingColumn
			continue
		}

		primaryKey, err := getPrimaryKey(s.db, s.configFromDsn.DBName, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key for table %q: %w", table, err)
		}

		tableKeys[table] = primaryKey
	}

	return tableKeys, nil
}
