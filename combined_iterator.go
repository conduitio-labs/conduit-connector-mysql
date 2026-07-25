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
	sdk "github.com/conduitio/conduit-connector-sdk"
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
	fetchSize             uint64
	startSnapshotPosition *common.SnapshotPosition
	startCdcPosition      *common.CdcPosition
	database              string
	canalRegexes          []string
	serverID              string
	mysqlConfig           *mysqldriver.Config
	disableCanalLogging   bool
	snapshotEnabled       bool
}

func newCombinedIterator(
	ctx context.Context,
	config combinedIteratorConfig,
) (common.Iterator, error) {
	cdcIterator, err := newCdcIterator(ctx, cdcIteratorConfig{
		tables:              config.canalRegexes,
		mysqlConfig:         config.mysqlConfig,
		tableKeys:           config.tableKeys,
		disableCanalLogging: config.disableCanalLogging,
		db:                  config.db,
		startPosition:       config.startCdcPosition,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc iterator: %w", err)
	}

	if !config.snapshotEnabled {
		if err := startCdcNoSnapshot(ctx, cdcIterator, config.startCdcPosition); err != nil {
			return nil, err
		}

		return &combinedIterator{
			cdcIterator:     cdcIterator,
			currentIterator: cdcIterator,
		}, nil
	}

	snapshotIterator, err := newSnapshotIterator(snapshotIteratorConfig{
		db:               config.db,
		tablePrimaryKeys: config.tableKeys,
		fetchSize:        config.fetchSize,
		startPosition:    config.startSnapshotPosition,
		database:         config.database,
		serverID:         config.serverID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot iterator: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("locking tables to setup fetch workers and obtain cdc start position")

	unlockTables, err := lockTables(ctx, config.db, config.tableKeys.GetTables())
	if err != nil {
		return nil, err
	}

	sdk.Logger(ctx).Info().Msg("locked tables")

	if err := snapshotIterator.setupWorkers(ctx); err != nil {
		return nil, err
	}

	sdk.Logger(ctx).Info().Msg("setup fetch workers")

	// Invariant 3: P0 (the cdc start position) must be captured or seeded here,
	// under the table lock and before any fetch worker starts reading, and
	// threaded into the snapshot iterator before it starts so that buildRecord
	// stamps it on every emitted record - including the very first one. See
	// docs/design-documents/20260724-snapshot-cdc-position-handoff.md.
	var p0 *common.CdcPosition
	switch {
	case config.startCdcPosition != nil:
		// Restart with an already-durable P0: either a mid-snapshot resume
		// (persisted as SnapshotPosition.CDCStart) or a steady-state CDC restart
		// (persisted as CdcPosition). Either way P0 is already known; no need to
		// capture a new one.
		p0 = config.startCdcPosition

	case len(snapshotIterator.workers) > 0:
		// Fresh snapshot phase with data to read: capture P0 now, still under the
		// lock, before unlocking and starting any worker.
		if err := cdcIterator.obtainStartPosition(); err != nil {
			return nil, fmt.Errorf("failed to fetch start cdc position: %w", err)
		}
		p0 = cdcIterator.position

		sdk.Logger(ctx).Info().Msg("fetched cdc start position")

	default:
		// All tables are empty: the snapshot phase will emit zero records, so
		// there is no record to durably carry P0 on. This is a CDC cold start
		// (see cdcIterator.startColdStart): unlock, then gate binlog replication
		// on a synthetic checkpoint record's ack instead.
		if err := unlockTables(); err != nil {
			return nil, err
		}
		sdk.Logger(ctx).Info().Msg("unlocked tables")

		if err := cdcIterator.startColdStart(ctx); err != nil {
			return nil, fmt.Errorf("failed to start cdc cold start: %w", err)
		}

		snapshotIterator.start(ctx) // no-op: zero workers, kept for symmetry/logging

		sdk.Logger(ctx).Info().Msg("started snapshot iterator (no tables to snapshot), cdc cold start pending checkpoint ack")

		return &combinedIterator{
			snapshotIterator: snapshotIterator,
			cdcIterator:      cdcIterator,
			currentIterator:  snapshotIterator,
		}, nil
	}

	if p0 == nil {
		// Unreachable: every branch above either assigns p0 or returns early.
		// Guards against a future edit silently dropping the P0 handoff and
		// emitting position-less snapshot records (Invariant 3).
		return nil, fmt.Errorf("internal error: no cdc start position available before starting a non-empty snapshot")
	}
	snapshotIterator.setCDCStart(p0)

	if err := unlockTables(); err != nil {
		return nil, err
	}

	sdk.Logger(ctx).Info().Msg("unlocked tables")

	snapshotIterator.start(ctx)

	sdk.Logger(ctx).Info().Msg("started snapshot iterator")

	if err := cdcIterator.start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start cdc iterator: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("started cdc iterator")

	iterator := &combinedIterator{
		snapshotIterator: snapshotIterator,
		cdcIterator:      cdcIterator,
		currentIterator:  snapshotIterator,
	}

	return iterator, nil
}

// startCdcNoSnapshot starts CDC when the snapshot phase is skipped entirely
// (snapshot.enabled=false). With no snapshot record to durably carry P0, a
// fresh start (startCdcPosition == nil) is a CDC cold start (Invariant 3): see
// cdcIterator.startColdStart. A restart with an already-durable startCdcPosition
// just resumes normally.
func startCdcNoSnapshot(ctx context.Context, cdcIterator *cdcIterator, startCdcPosition *common.CdcPosition) error {
	if startCdcPosition == nil {
		if err := cdcIterator.startColdStart(ctx); err != nil {
			return fmt.Errorf("failed to start cdc cold start: %w", err)
		}
		sdk.Logger(ctx).Info().Msg("skipped table snapshot, cdc cold start pending checkpoint ack")
		return nil
	}

	if err := cdcIterator.start(ctx); err != nil {
		return fmt.Errorf("failed to start cdc iterator: %w", err)
	}
	sdk.Logger(ctx).Info().Msg("skipped table snapshot and started cdc iterator")
	return nil
}

func (c *combinedIterator) Ack(ctx context.Context, pos opencdc.Position) error {
	//nolint:wrapcheck // error already wrapped in iterator
	return c.currentIterator.Ack(ctx, pos)
}

func (c *combinedIterator) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	recs, err := c.currentIterator.ReadN(ctx, n)
	if errors.Is(err, ErrSnapshotIteratorDone) {
		c.currentIterator = c.cdcIterator
		//nolint:wrapcheck // error already wrapped in iterator
		return c.currentIterator.ReadN(ctx, n)
	} else if err != nil {
		return nil, fmt.Errorf("failed to get next record: %w", err)
	}

	return recs, nil
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

func lockTables(ctx context.Context, db *sqlx.DB, tables []string) (func() error, error) {
	tableList := strings.Join(tables, ", ")

	_, err := db.ExecContext(ctx, "FLUSH TABLES "+tableList+" WITH READ LOCK")
	if err != nil {
		return nil, fmt.Errorf("failed to flush table list '%s' and acquire lock: %w", tableList, err)
	}

	return func() error {
		if _, err := db.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			return fmt.Errorf("failed to unlock tables after getting cdc position: %w", err)
		}
		return nil
	}, nil
}
