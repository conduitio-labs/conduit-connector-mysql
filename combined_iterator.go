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
	"errors"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type combinedIterator struct {
	snapshotIterator common.Iterator
	cdcIterator      common.Iterator

	currentIterator common.Iterator
}

type combinedIteratorConfig struct {
	db                    *sqlx.DB
	tableKeys             common.TableKeys
	fetchSize             int
	startSnapshotPosition *common.SnapshotPosition
	startCdcPosition      *common.CdcPosition
	database              string
	tables                []string
	serverID              common.ServerID
	mysqlConfig           *mysqldriver.Config
	disableCanalLogging   bool
}

func newCombinedIterator(
	ctx context.Context,
	config combinedIteratorConfig,
) (common.Iterator, error) {
	cdcIterator, err := newCdcIterator(ctx, cdcIteratorConfig{
		tables:              config.tables,
		mysqlConfig:         config.mysqlConfig,
		tableKeys:           config.tableKeys,
		disableCanalLogging: config.disableCanalLogging,
		db:                  config.db,
		startPosition:       config.startCdcPosition,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc iterator: %w", err)
	}

	snapshotIterator, err := newSnapshotIterator(snapshotIteratorConfig{
		db:            config.db,
		tableKeys:     config.tableKeys,
		fetchSize:     config.fetchSize,
		startPosition: config.startSnapshotPosition,
		database:      config.database,
		tables:        config.tables,
		serverID:      config.serverID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot iterator: %w", err)
	}

	unlockTables, err := lockTables(ctx, config)
	if err != nil {
		return nil, err
	}

	if err := snapshotIterator.setupWorkers(ctx); err != nil {
		return nil, err
	}

	if config.startCdcPosition == nil {
		if err := cdcIterator.obtainStartPosition(ctx); err != nil {
			return nil, fmt.Errorf("failed to fetch start cdc position: %w", err)
		}
	}

	if err := unlockTables(); err != nil {
		return nil, err
	}

	snapshotIterator.start(ctx)

	if err := cdcIterator.start(); err != nil {
		return nil, fmt.Errorf("failed to start cdc iterator: %w", err)
	}

	iterator := &combinedIterator{
		snapshotIterator: snapshotIterator,
		cdcIterator:      cdcIterator,
		currentIterator:  snapshotIterator,
	}

	return iterator, nil
}

func (c *combinedIterator) Ack(ctx context.Context, pos opencdc.Position) error {
	//nolint:wrapcheck // error already wrapped in iterator
	return c.currentIterator.Ack(ctx, pos)
}

func (c *combinedIterator) Next(ctx context.Context) (opencdc.Record, error) {
	rec, err := c.currentIterator.Next(ctx)
	if errors.Is(err, ErrSnapshotIteratorDone) {
		c.currentIterator = c.cdcIterator
		//nolint:wrapcheck // error already wrapped in iterator
		return c.currentIterator.Next(ctx)
	} else if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to get next record: %w", err)
	}

	return rec, nil
}

func (c *combinedIterator) Teardown(ctx context.Context) error {
	var errs []error

	if c.snapshotIterator != nil {
		err := c.snapshotIterator.Teardown(ctx)
		errs = append(errs, err)
	}

	if c.cdcIterator != nil {
		err := c.cdcIterator.Teardown(ctx)
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func lockTables(
	ctx context.Context,
	config combinedIteratorConfig,
) (func() error, error) {
	tableList := strings.Join(config.tables, ", ")

	_, err := config.db.ExecContext(ctx, "FLUSH TABLES "+tableList+" WITH READ LOCK")
	if err != nil {
		return nil, fmt.Errorf("failed to flush tables and acquire lock: %w", err)
	}

	return func() error {
		if _, err := config.db.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			return fmt.Errorf("failed to unlock tables after getting cdc position: %w", err)
		}
		return nil
	}, nil
}
