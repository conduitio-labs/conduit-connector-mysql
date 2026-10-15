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
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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

	types *typeSettings
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

	// coverage is stamped on CDC positions; see resolveCoverage.
	coverage *common.TableCoverage

	// types is the resolved type configuration; nil is legacy.
	types *typeSettings
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
		coverage:            config.coverage,
		types:               config.types,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc iterator: %w", err)
	}

	if !config.snapshotEnabled {
		if err := cdcIterator.start(ctx); err != nil {
			return nil, fmt.Errorf("failed to start cdc iterator: %w", err)
		}

		sdk.Logger(ctx).Info().Msg("skipped table snapshot and started cdc iterator")

		iterator := &combinedIterator{
			cdcIterator:     cdcIterator,
			currentIterator: cdcIterator,
			types:           config.types,
		}

		return iterator, nil
	}

	snapshotIterator, err := newSnapshotIterator(snapshotIteratorConfig{
		db:               config.db,
		tablePrimaryKeys: config.tableKeys,
		fetchSize:        config.fetchSize,
		startPosition:    config.startSnapshotPosition,
		database:         config.database,
		serverID:         config.serverID,
		types:            config.types,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot iterator: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("locking tables to setup fetch workers and obtain cdc start position")

	unlockTables, err := lockTables(ctx, config.db, config.tableKeys.GetTables())
	if err != nil {
		return nil, err
	}
	// Every error return below must release the lock: a returned error leaves no
	// iterator to do it later, and writers stay blocked on the locked tables.
	// unlockTables is idempotent. The explicit call further down returns its own
	// error to the caller; unlockReported stops this deferred call from logging
	// the same failure a second time.
	unlockReported := false
	defer func() {
		if unlockReported {
			return
		}
		if err := unlockTables(); err != nil {
			sdk.Logger(ctx).Warn().Err(err).Msg("failed to release table lock")
		}
	}()

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
	if config.startCdcPosition != nil {
		// Restart with an already-durable P0: either a mid-snapshot resume
		// (persisted as SnapshotPosition.CDCStart) or a steady-state CDC restart
		// (persisted as CdcPosition). Either way P0 is already known; no need to
		// capture a new one.
		p0 = config.startCdcPosition
	} else {
		// Fresh snapshot phase: capture P0 now, still under the lock, before
		// unlocking and starting any worker. This runs regardless of whether any
		// table actually has rows to snapshot - a snapshot over all-empty tables
		// still needs P0 threaded through for CDC to start from afterward.
		if err := cdcIterator.obtainStartPosition(); err != nil {
			return nil, fmt.Errorf("failed to fetch start cdc position: %w", err)
		}
		p0 = cdcIterator.position

		sdk.Logger(ctx).Info().Msg("fetched cdc start position")
	}
	snapshotIterator.setCDCStart(p0)

	unlockReported = true
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
		types:            config.types,
	}

	return iterator, nil
}

func (c *combinedIterator) Ack(ctx context.Context, pos opencdc.Position) error {
	// The handoff to CDC happens only after every snapshot record is acked,
	// so an ack that arrives while CDC is current is for a CDC record.
	phase := phaseSnapshot
	if c.currentIterator == c.cdcIterator {
		phase = phaseCDC
	}
	if from := c.types.observeAck(phase); from != "" {
		sdk.Logger(ctx).Info().
			Str("from", from).
			Str("to", c.types.config.name()).
			Msg("a record under the new type configuration was acknowledged; types.acceptChangeFrom can be removed")
	}
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

// lockTables takes a global read lock on tables and returns the function that
// releases it.
//
// FLUSH TABLES ... WITH READ LOCK and UNLOCK TABLES are session-scoped, so both
// must run on the same connection. They previously went through the sql.DB pool,
// where the UNLOCK could be handed a different connection, do nothing, and leave
// every table read-locked on the idle connection that took the lock until the
// pool closed. The lock now lives on one dedicated connection, and the returned
// function is safe to call more than once: it releases the lock on the first
// call, and a connection whose UNLOCK failed is discarded rather than returned to
// the pool still holding the lock (closing the session releases it).
func lockTables(ctx context.Context, db *sqlx.DB, tables []string) (func() error, error) {
	tableList := strings.Join(tables, ", ")

	conn, err := db.Connx(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get a dedicated connection to lock tables: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "FLUSH TABLES "+tableList+" WITH READ LOCK"); err != nil {
		discardConn(conn)
		return nil, fmt.Errorf("failed to flush table list '%s' and acquire lock: %w", tableList, err)
	}

	var (
		once      sync.Once
		unlockErr error
	)
	return func() error {
		once.Do(func() {
			// Not the caller's context: a cancelled context must not leave the
			// lock behind. Closing the session also releases it.
			unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()

			if _, err := conn.ExecContext(unlockCtx, "UNLOCK TABLES"); err != nil {
				unlockErr = fmt.Errorf("failed to unlock tables after getting cdc position: %w", err)
				discardConn(conn)
				return
			}
			if err := conn.Close(); err != nil {
				unlockErr = fmt.Errorf("failed to release the lock connection: %w", err)
			}
		})
		return unlockErr
	}, nil
}

// discardConn closes the underlying connection instead of returning it to the
// pool, which ends its session and with it any lock it holds.
func discardConn(conn *sqlx.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}
