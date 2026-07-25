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
	"strconv"
	"sync"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// ErrCDCStartPositionUnavailable is a stable, actionable error returned when the
// binlog file a CDC resume requires is no longer available on the server (e.g.
// purged by binlog_expire_logs_seconds). Before this fix, this failure mode
// stalled ReadN forever instead of surfacing an error; see the Decision section
// of docs/design-documents/20260724-snapshot-cdc-position-handoff.md.
var ErrCDCStartPositionUnavailable = errors.New("cdc start position unavailable")

// coldStartAckLogInterval controls how often ReadN logs while blocked waiting for
// the cold-start checkpoint record to be acked (see startColdStart).
const coldStartAckLogInterval = 30 * time.Second

type cdcIterator struct {
	config   cdcIteratorConfig
	canal    *canal.Canal
	position *common.CdcPosition

	canalDoneC     chan struct{}
	parsedRecordsC chan opencdc.Record

	// canalRunDoneC is closed exactly once, when canal.RunFrom returns; canalRunErr
	// holds its return value. Closing (rather than sending on an unbuffered
	// channel) lets both ReadN and Teardown independently observe the same
	// outcome without racing each other for a single delivery - see wrapCanalRunErr
	// and Teardown.
	canalRunDoneC chan struct{}
	canalRunErr   error

	// Cold-start checkpoint gating (Invariant 3, see startColdStart). Populated
	// only when CDC starts with no persisted or snapshot-carried P0 (an empty
	// snapshot, or snapshot.enabled=false). checkpointC is buffered(1) and holds
	// the pending checkpoint record until ReadN emits it; a nil channel (the
	// normal, non-cold-start case) is never selectable, so the ReadN select below
	// degrades to a no-op in that case.
	checkpointC       chan opencdc.Record
	checkpointAckedC  chan struct{}
	checkpointAckOnce sync.Once
	// checkpointAckErr holds runFrom's result from inside checkpointAckOnce.Do.
	// It must live on the iterator, not as a local in Ack: sync.Once guarantees
	// the closure's completion happens-before every call to Do returns (not just
	// the one that ran it), but a local variable is only visible to the
	// goroutine that declared it. A concurrent Ack call that loses the Do race
	// would otherwise always observe a nil error, even if runFrom actually
	// failed.
	checkpointAckErr error
	// coldStartCtx is the long-lived context captured at startColdStart, used to
	// launch binlog replication from Ack once the checkpoint is acked (Ack's own
	// context is request-scoped and not appropriate for a background goroutine
	// that outlives the Ack call).
	coldStartCtx context.Context //nolint:containedctx // see comment above

	// runGate coordinates the race between launching binlog replication and
	// tearing the iterator down. See cdcRunGate's doc comment.
	runGate cdcRunGate
}

// cdcRunGate makes two things true about launching binlog replication
// (canal.SetEventHandler + canal.RunFrom) versus tearing the iterator down
// (canal.Close):
//
//  1. canal.SetEventHandler must never run concurrently with canal.Close.
//     Confirmed at the vendored library level (go-mysql-org/go-mysql@v1.14.0):
//     SetEventHandler (handler.go:67, "c.eventHandler = h") is an
//     unsynchronized field write, while Close (canal.go:258-273) reads that
//     same field under its own internal lock
//     ("c.eventHandler.OnPosSynced(...)"). Concurrent calls are a genuine data
//     race.
//  2. Teardown must never block waiting for a launch that will never happen.
//     Pre-cold-start, start() always launched replication unconditionally at
//     iterator construction, long before Teardown could possibly run, so
//     waiting was always safe. Cold start broke that invariant: launching is
//     deferred to Ack (once the checkpoint record is acked), which may never
//     come - destination down, pipeline stopped early - and Ack can race a
//     concurrent, graceful Teardown.
//
// Both are achieved with a single mutex: setup (SetEventHandler) and close
// (Close) are only ever invoked from inside tryLaunch/launchUnconditionally
// and teardown respectively, all while holding the same lock, so they can
// never overlap in time regardless of call order. teardown() itself never
// blocks - it only reports whether a launch was committed to, so the caller
// (cdcIterator.Teardown) knows whether it's safe (and necessary) to wait for
// replication to actually stop.
//
// It is deliberately canal-independent (setup/close are passed in as
// closures) so this concurrency behavior can be unit-tested under -race
// without a live MySQL connection, which constructing a real *canal.Canal
// requires - see cdc_run_gate_test.go.
type cdcRunGate struct {
	mu       sync.Mutex
	tornDown bool
	launched bool
}

// tryLaunch attempts to commit to a launch. If teardown has already run (or is
// running), it returns false and setup is never called - not launching is
// always a safe, lossless choice for the cold-start caller (see Ack). Otherwise
// it marks the gate launched and calls setup while still holding the lock, and
// returns true.
func (g *cdcRunGate) tryLaunch(setup func()) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.tornDown {
		return false
	}
	g.launched = true
	setup()
	return true
}

// launchUnconditionally marks the gate launched and calls setup while holding
// the lock, without checking tornDown. Use only when the caller is guaranteed
// to run before teardown can possibly be invoked (iterator construction). It
// always returns true; the return value exists so it can be used interchangeably
// with tryLaunch (see runFrom's commit parameter).
func (g *cdcRunGate) launchUnconditionally(setup func()) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.launched = true
	setup()
	return true
}

// teardown marks the gate torn down - so no future tryLaunch call can ever
// launch anything - and calls closeFn while holding the same lock used by
// tryLaunch/launchUnconditionally's setup, so the two can never run
// concurrently. It returns whether a launch was committed to, so the caller
// knows whether to wait for it to finish.
func (g *cdcRunGate) teardown(closeFn func()) (launched bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.tornDown = true
	closeFn()
	return g.launched
}

type cdcIteratorConfig struct {
	db                  *sqlx.DB
	tables              []string
	mysqlConfig         *mysqldriver.Config
	tableKeys           common.TableKeys
	disableCanalLogging bool
	startPosition       *common.CdcPosition
}

func newCdcIterator(ctx context.Context, config cdcIteratorConfig) (*cdcIterator, error) {
	canal, err := common.NewCanal(ctx, common.CanalConfig{
		Config:         config.mysqlConfig,
		Tables:         config.tables,
		DisableLogging: config.disableCanalLogging,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start canal at combined iterator: %w", err)
	}

	return &cdcIterator{
		config:         config,
		canal:          canal,
		position:       config.startPosition,
		canalRunDoneC:  make(chan struct{}),
		canalDoneC:     make(chan struct{}),
		parsedRecordsC: make(chan opencdc.Record),
	}, nil
}

func (c *cdcIterator) obtainStartPosition() error {
	masterPos, err := c.canal.GetMasterPos()
	if err != nil {
		return fmt.Errorf("failed to get mysql master position after acquiring locks: %w", err)
	}

	c.position = &common.CdcPosition{
		ReplicationEventPosition: common.ReplicationEventPosition{
			Name: masterPos.Name,
			Pos:  masterPos.Pos,
		},
	}

	return nil
}

// start begins binlog replication immediately from the iterator's start position
// (set by obtainStartPosition, or seeded from a persisted position). Use this for
// every case except a true CDC cold start; see startColdStart for that case.
//
// start is only ever called synchronously during iterator construction, well
// before Teardown can run, so it launches unconditionally (unlike the
// cold-start Ack path, there is no race with Teardown to guard against here -
// see runGate).
func (c *cdcIterator) start(ctx context.Context) error {
	return c.runFrom(ctx, c.runGate.launchUnconditionally)
}

// startColdStart handles CDC starting with no persisted or snapshot-carried P0
// (an empty snapshot, or snapshot.enabled=false): every table empty, or
// snapshot.enabled=false.
//
// Invariant 3: on CDC cold-start, persist P0 (via the checkpoint record's ack)
// BEFORE advancing the binlog read past P0. A fresh master position cannot
// recover binlog history it skips, so the read must not advance until P0 is
// durable.
//
// P0 is captured here, but binlog replication (runFrom) is deliberately NOT
// started yet. Instead a synthetic checkpoint record carrying P0 is queued for
// ReadN to emit as the very next record; Ack starts replication only once that
// record is acked (see ReadN, Ack). If the crash happens before the ack, the
// persisted position is still nil/absent and a restart simply re-captures a
// fresh P0' - nothing was read past P0, so nothing is lost. Only after the ack
// does the read advance, at which point P0 is the durable floor.
func (c *cdcIterator) startColdStart(ctx context.Context) error {
	masterPos, err := c.canal.GetMasterPos()
	if err != nil {
		return fmt.Errorf("failed to get mysql master position for cdc cold start: %w", err)
	}

	p0 := common.CdcPosition{
		ReplicationEventPosition: common.ReplicationEventPosition{
			Name: masterPos.Name,
			Pos:  masterPos.Pos,
		},
	}
	c.position = &p0
	c.coldStartCtx = ctx
	c.checkpointAckedC = make(chan struct{})
	c.checkpointC = make(chan opencdc.Record, 1)
	c.checkpointC <- newColdStartCheckpointRecord(p0)

	sdk.Logger(ctx).Info().
		Str("binlog_file", p0.Name).
		Uint32("binlog_pos", p0.Pos).
		Msg("cdc cold start: emitting synthetic checkpoint record to durably persist the cdc start " +
			"position before binlog replication begins; binlog replication will not start until it is acked")

	return nil
}

// runFrom launches binlog replication in the background from the iterator's
// current start position. It is the shared implementation behind start
// (commit = runGate.launchUnconditionally: always safe, called synchronously at
// construction) and the cold-start Ack handler (commit = runGate.tryLaunch:
// must check whether Teardown has already run - see Ack).
//
// SetEventHandler is called synchronously, from inside commit, before this
// function spawns any goroutine - not deferred into the background goroutine.
// That gives Teardown a clean happens-before boundary: by the time runFrom
// returns, SetEventHandler has either already run (commit returned true) or
// never will (commit returned false; see runGate). Only the actual blocking
// replication call (canal.RunFrom) happens in the background.
func (c *cdcIterator) runFrom(ctx context.Context, commit func(setup func()) bool) error {
	startPosition, err := c.getStartPosition()
	if err != nil {
		return fmt.Errorf("failed to get start position: %w", err)
	}

	eventHandler := newCdcEventHandler(
		ctx,
		c.canal,
		c.canalDoneC,
		c.parsedRecordsC,
		c.config.tableKeys,
		startPosition,
	)

	launched := commit(func() {
		c.canal.SetEventHandler(eventHandler)
	})
	if !launched {
		// Teardown has already run (or is running); see cdcRunGate. Nothing was
		// read past P0, so this is a safe, lossless no-op - not an error.
		return nil
	}

	go func() {
		// We need to run canal from Previous position to be sure
		// we didn't lose any record from multi-row mysql replication
		// event.
		pos := startPosition.ReplicationEventPosition
		if startPosition.PrevPosition != nil {
			pos = *startPosition.PrevPosition
		}

		c.canalRunErr = c.canal.RunFrom(pos.ToMysqlPos())
		close(c.canalRunDoneC)
	}()

	return nil
}

func (c *cdcIterator) getStartPosition() (common.CdcPosition, error) {
	if c.position != nil {
		return *c.position, nil
	}

	var cdcPosition common.CdcPosition
	masterPos, err := c.canal.GetMasterPos()
	if err != nil {
		return cdcPosition, fmt.Errorf("failed to get master position: %w", err)
	}

	return common.CdcPosition{
		ReplicationEventPosition: common.ReplicationEventPosition{
			Name: masterPos.Name,
			Pos:  masterPos.Pos,
		},
	}, nil
}

// Ack is a no-op for ordinary CDC records: acks are only meaningful for
// checkpointing, and CDC positions are self-contained in each record. The one
// exception is the cold-start synthetic checkpoint record (see startColdStart):
// its ack is the durability signal that P0 is now safely persisted, which is
// what gates starting binlog replication.
func (c *cdcIterator) Ack(_ context.Context, _ opencdc.Position) error {
	if c.checkpointAckedC == nil {
		return nil
	}

	c.checkpointAckOnce.Do(func() {
		// runGate.tryLaunch checks whether Teardown has already run before
		// committing to a launch - see cdcRunGate's doc comment. If it hasn't,
		// runFrom returns (false, nil): nothing was read past P0 (replication
		// never launched), which is a lossless outcome, not an error - a restart
		// simply re-runs cold start and re-captures P0. A real failure (e.g.
		// getStartPosition erroring) is reported via err regardless of launched,
		// so it is never swallowed alongside the torn-down case.
		c.checkpointAckErr = c.runFrom(c.coldStartCtx, c.runGate.tryLaunch)
		close(c.checkpointAckedC)
	})
	if c.checkpointAckErr != nil {
		return fmt.Errorf("failed to start cdc replication after cold-start checkpoint ack: %w", c.checkpointAckErr)
	}
	return nil
}

func (c *cdcIterator) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	// Emit the pending cold-start checkpoint record, if any, before anything
	// else. checkpointC is nil outside cold start, so this case is simply never
	// selectable then.
	select {
	case rec := <-c.checkpointC:
		return []opencdc.Record{rec}, nil
	default:
	}

	if c.checkpointAckedC != nil {
		if err := c.waitForCheckpointAck(ctx); err != nil {
			return nil, err
		}
	}

	var recs []opencdc.Record

	// Block until at least one record is received or context is canceled
	select {
	case <-ctx.Done():
		//nolint:wrapcheck // no need to wrap canceled error
		return nil, ctx.Err()
	case <-c.canalDoneC:
		return nil, fmt.Errorf("canal is closed")
	case <-c.canalRunDoneC:
		return nil, c.wrapCanalRunErr()
	case rec := <-c.parsedRecordsC:
		recs = append(recs, rec)
	}

	// try getting the remaining (n-1) records without blocking
	for len(recs) < n {
		select {
		case rec := <-c.parsedRecordsC:
			recs = append(recs, rec)
		case <-ctx.Done():
			//nolint:wrapcheck // no need to wrap canceled error
			return recs, ctx.Err()
		case <-c.canalDoneC:
			return recs, fmt.Errorf("canal is closed")
		case <-c.canalRunDoneC:
			if len(recs) > 0 {
				// Deliver what we already have; the error surfaces on the next call.
				return recs, nil
			}
			return recs, c.wrapCanalRunErr()
		default:
			// No more records currently available
			return recs, nil
		}
	}

	return recs, nil
}

// waitForCheckpointAck blocks ReadN until the cold-start checkpoint record has
// been acked, logging periodically so operators can see why the connector is not
// making progress (e.g. because the destination is down). There is deliberately
// no timeout: advancing without a durable P0 would reopen the exact data-loss
// window this fix closes. See the Open questions / Decision log in
// docs/design-documents/20260724-snapshot-cdc-position-handoff.md.
//
// It also unblocks on canalDoneC, which Teardown always closes as its first
// action regardless of whether the checkpoint was ever acked: if the pipeline
// stops gracefully while the checkpoint is still pending, this goroutine must
// not leak waiting for an ack that will now never come.
func (c *cdcIterator) waitForCheckpointAck(ctx context.Context) error {
	select {
	case <-c.checkpointAckedC:
		return nil
	case <-c.canalDoneC:
		return fmt.Errorf("canal is closed")
	default:
	}

	ticker := time.NewTicker(coldStartAckLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			//nolint:wrapcheck // no need to wrap canceled error
			return ctx.Err()
		case <-c.checkpointAckedC:
			return nil
		case <-c.canalDoneC:
			return fmt.Errorf("canal is closed")
		case <-ticker.C:
			sdk.Logger(ctx).Warn().
				Str("binlog_file", c.position.Name).
				Uint32("binlog_pos", c.position.Pos).
				Msg("cdc cold start: still waiting for the checkpoint record to be acked before starting " +
					"binlog replication; this is expected if the destination is unavailable and does not " +
					"indicate data loss - replication will begin as soon as the checkpoint is acked")
		}
	}
}

// wrapCanalRunErr turns canal.RunFrom's return value into a stable, actionable
// error. A graceful Teardown-triggered close is reported as such; any other
// error (most commonly a purged/expired binlog file) is reported as
// ErrCDCStartPositionUnavailable, naming the binlog file and position that could
// not be resumed from. Before this fix this failure mode was never surfaced:
// ReadN blocked forever and the error was only drained (and discarded into the
// Teardown error, if any) once Teardown ran. See the Decision section of
// docs/design-documents/20260724-snapshot-cdc-position-handoff.md.
func (c *cdcIterator) wrapCanalRunErr() error {
	err := c.canalRunErr
	if err == nil {
		return errors.New("canal is closed")
	}
	if errors.Is(err, replication.ErrSyncClosed) {
		return fmt.Errorf("canal is closed: %w", err)
	}

	var name string
	var pos uint32
	if c.position != nil {
		name, pos = c.position.Name, c.position.Pos
	}

	return fmt.Errorf(
		"%w: cannot resume cdc replication from binlog file %q at position %d - the binlog is likely "+
			"purged or expired; increase binlog retention (binlog_expire_logs_seconds) or clear the "+
			"connector's position to trigger a fresh snapshot: %w",
		ErrCDCStartPositionUnavailable, name, pos, err)
}

// Teardown stops the iterator. On a cold start, runFrom may never have been
// launched (the checkpoint record was never acked - e.g. the destination was
// down, or the pipeline was stopped early). Two things must hold in that case:
//
//   - Teardown must not hang: it must not wait on canalRunDoneC, since nothing
//     will ever close it if no goroutine was launched to do so.
//   - canal.Close() must never run concurrently with the runFrom goroutine's
//     SetEventHandler call (see the mu doc comment on cdcIterator) - so
//     tornDown/runFromLaunched are read and written, and canal.Close() is
//     called, all under the same mu that guards SetEventHandler.
func (c *cdcIterator) Teardown(ctx context.Context) error {
	// Unblocks any goroutine parked in ReadN/waitForCheckpointAck waiting on a
	// checkpoint ack that will now never come, regardless of the launched/
	// tornDown outcome below.
	close(c.canalDoneC)

	// runGate.teardown never blocks; it reports whether a launch was (or, since
	// tornDown is now set, ever will be) committed to - see cdcRunGate's doc
	// comment.
	launched := c.runGate.teardown(c.canal.Close)

	if !launched {
		// runFrom was never launched and never will be. There is nothing that
		// will close canalRunDoneC, so don't wait on it - doing so would hang
		// forever.
		return nil
	}

	select {
	case <-ctx.Done():
		//nolint:wrapcheck // no need to wrap canceled error
		return ctx.Err()
	case <-c.canalRunDoneC:
		err := c.canalRunErr
		if errors.Is(err, replication.ErrSyncClosed) {
			// Using error level might be too much.
			sdk.Logger(ctx).Warn().Err(err).Msg("error found when closing mysql canal")
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to stop canal: %w", err)
		}
	}

	return nil
}

// newColdStartCheckpointRecord builds the synthetic record that durably persists
// P0 at CDC cold start (see startColdStart). It reuses the delete operation with
// an empty/tombstone payload (no before, no after) so it introduces no new
// opencdc record shape; it is distinguished purely by the
// common.CheckpointMetadataKey metadata key, which downstream filters can match
// on to skip it. Its Position is P0 itself, so once acked, a restart parses
// pos.CdcPosition != nil and takes the ordinary steady-state resume path - no
// second checkpoint record is ever emitted for the same cold start.
func newColdStartCheckpointRecord(p0 common.CdcPosition) opencdc.Record {
	metadata := opencdc.Metadata{}
	metadata.SetCreatedAt(time.Now().UTC())
	metadata[common.CheckpointMetadataKey] = "true"

	key := opencdc.RawData(fmt.Sprintf("mysql.checkpoint.%s.%d", p0.Name, p0.Pos))

	return sdk.Util.Source.NewRecordDelete(p0.ToSDKPosition(), metadata, key, nil)
}

type replicationEventRow struct {
	before []any
	after  []any
}

type rowEvent struct {
	*canal.RowsEvent
	Rows []replicationEventRow
}

type onRowChangeFn func(rowEvent) ([]opencdc.Record, error)

type cdcEventHandler struct {
	canal.DummyEventHandler
	canal *canal.Canal

	canalDoneC     chan struct{}
	parsedRecordsC chan opencdc.Record

	tablePrimaryKeys common.TableKeys

	onRowsChange onRowChangeFn
}

func newCdcEventHandler(
	ctx context.Context,
	canal *canal.Canal,
	canalDoneC chan struct{},
	parsedRecordsC chan opencdc.Record,
	tablesPrimaryKeys common.TableKeys,
	startPosition common.CdcPosition,
) *cdcEventHandler {
	h := &cdcEventHandler{
		canal:            canal,
		canalDoneC:       canalDoneC,
		parsedRecordsC:   parsedRecordsC,
		tablePrimaryKeys: tablesPrimaryKeys,
	}

	h.onRowsChange = h.handleSingleRowChange(ctx, startPosition)

	return h
}

func (h *cdcEventHandler) createMetadata(
	ctx context.Context,
	e rowEvent,
	keySchema *schemaMapper,
	payloadSchema *schemaMapper,
) (opencdc.Metadata, error) {
	payloadAvroCols := make([]*avroNamedType, len(e.Table.Columns))
	for i, col := range e.Table.Columns {
		avroCol, err := mysqlSchemaToAvroCol(col)
		if err != nil {
			return nil, fmt.Errorf("failed to parse avro cols: %w", err)
		}
		payloadAvroCols[i] = avroCol
	}

	tableName := e.Table.Name

	payloadSubver, err := payloadSchema.createPayloadSchema(ctx, tableName, payloadAvroCols)
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc payload schema for table %s: %w", tableName, err)
	}

	metadata := opencdc.Metadata{}
	metadata.SetCollection(e.Table.Name)
	metadata.SetCreatedAt(time.Unix(int64(e.Header.Timestamp), 0).UTC())
	metadata[common.ServerIDKey] = strconv.FormatUint(uint64(e.Header.ServerID), 10)

	metadata.SetPayloadSchemaSubject(payloadSubver.subject)
	metadata.SetPayloadSchemaVersion(payloadSubver.version)

	if keyCols := h.tablePrimaryKeys[tableName]; len(keyCols) != 0 {
		keyAvroCols := make([]*avroNamedType, 0, len(keyCols))
		for _, keyCol := range keyCols {
			keyColType, found := findKeyColType(payloadAvroCols, keyCol)
			if !found {
				return nil, fmt.Errorf("failed to find key schema column type for table %s", tableName)
			}
			keyAvroCols = append(keyAvroCols, keyColType)
		}

		keySubver, err := keySchema.createKeySchema(ctx, tableName, keyAvroCols)
		if err != nil {
			return nil, fmt.Errorf("failed to create key schema for table %s: %w", tableName, err)
		}

		metadata.SetKeySchemaSubject(keySubver.subject)
		metadata.SetKeySchemaVersion(keySubver.version)
	}

	return metadata, nil
}

func (h *cdcEventHandler) buildKey(
	ctx context.Context,
	e rowEvent,
	payload opencdc.StructuredData,
	keySchema *schemaMapper,
) opencdc.Data {
	keyCols := h.tablePrimaryKeys[e.Table.Name]

	if len(keyCols) == 0 {
		keyVal := fmt.Sprintf("%s_%d", h.canal.SyncedPosition().Name, e.Header.LogPos)
		return opencdc.RawData(keyVal)
	}

	key := opencdc.StructuredData{}

	for _, keyCol := range keyCols {
		keyVal := keySchema.formatValue(ctx, keyCol, payload[keyCol])
		key[keyCol] = keyVal
	}

	return key
}

func (h *cdcEventHandler) buildRecords(
	ctx context.Context,
	e rowEvent,
	prevPos common.ReplicationEventPosition,
) ([]opencdc.Record, error) {
	keySchema := newSchemaMapper()
	payloadSchema := newSchemaMapper()

	metadata, err := h.createMetadata(ctx, e, keySchema, payloadSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata: %w", err)
	}

	records := make([]opencdc.Record, 0, len(e.Rows))
	for i, row := range e.Rows {
		payloadAfter := h.buildPayload(ctx, payloadSchema, e.Table.Columns, row.after)
		key := h.buildKey(ctx, e, payloadAfter, keySchema)

		var payloadBefore opencdc.StructuredData
		if row.before != nil {
			payloadBefore = h.buildPayload(ctx, payloadSchema, e.Table.Columns, row.before)
		}

		pos := common.CdcPosition{
			ReplicationEventPosition: common.ReplicationEventPosition{
				Name: h.canal.SyncedPosition().Name,
				Pos:  e.Header.LogPos,
			},
			PrevPosition: &prevPos,
			Index:        i,
		}.ToSDKPosition()

		var rec opencdc.Record
		switch e.Action {
		case canal.InsertAction:
			rec = sdk.Util.Source.NewRecordCreate(pos, metadata, key, payloadAfter)
		case canal.UpdateAction:
			rec = sdk.Util.Source.NewRecordUpdate(pos, metadata, key, payloadBefore, payloadAfter)
		case canal.DeleteAction:
			rec = sdk.Util.Source.NewRecordDelete(pos, metadata, key, payloadAfter)
		}

		records = append(records, rec)
	}

	return records, nil
}

func findKeyColType(avroCols []*avroNamedType, keyCol string) (*avroNamedType, bool) {
	for _, avroCol := range avroCols {
		if keyCol == avroCol.Name {
			return avroCol, true
		}
	}
	return nil, false
}

func (h *cdcEventHandler) buildPayload(
	ctx context.Context,
	payloadSchema *schemaMapper,
	columns []schema.TableColumn, rows []any,
) opencdc.StructuredData {
	payload := opencdc.StructuredData{}
	for i, col := range columns {
		payload[col.Name] = payloadSchema.formatValue(ctx, col.Name, rows[i])
	}
	return payload
}

func (h *cdcEventHandler) handleSingleRowChange(
	ctx context.Context,
	startPosition common.CdcPosition,
) onRowChangeFn {
	// MySQL replication event could contain multiple rows
	// with the same position.
	// The Index identifier describes the row index in such an event.
	// Here we need to start replication from an absolute position,
	// including the row index.
	requiredOffset := startPosition.Index + 1

	// If there was no prev position, we started replication
	// from the very beginning => don't try to skip any records.
	if startPosition.PrevPosition == nil {
		requiredOffset = 0
	}

	prevPosition := common.ReplicationEventPosition{
		Name: startPosition.Name,
		Pos:  startPosition.Pos,
	}

	return func(e rowEvent) ([]opencdc.Record, error) {
		if len(e.Rows) < requiredOffset {
			// should be impossible
			sdk.Logger(ctx).Error().
				Any("position", startPosition).
				Msg("unexpected number of rows in the event: some records could be lost")
		}

		e.Rows = e.Rows[requiredOffset:]

		// Only a part of the first event could be skipped.
		requiredOffset = 0

		rows, err := h.buildRecords(ctx, e, prevPosition)

		prevPosition = common.ReplicationEventPosition{
			Name: h.canal.SyncedPosition().Name,
			Pos:  e.Header.LogPos,
		}

		return rows, err
	}
}

func (h *cdcEventHandler) OnRow(e *canal.RowsEvent) error {
	rowEvent := rowEvent{
		RowsEvent: e,
		Rows:      make([]replicationEventRow, 0, len(e.Rows)),
	}

	if e.Action == canal.UpdateAction && len(e.Rows)%2 != 0 {
		return fmt.Errorf("even number of rows is expected in replication event")
	}

	for i := 0; i < len(e.Rows); i++ {
		row := replicationEventRow{}
		switch e.Action {
		case canal.InsertAction, canal.DeleteAction:
			row.after = e.Rows[i]
		case canal.UpdateAction:
			// updated rows are going in pairs:
			// [ row_before, row_after, row_before, row_after ]
			row.before = e.Rows[i]
			row.after = e.Rows[i+1]
			i++
		default:
			return fmt.Errorf("unknown action type: %v", e.Action)
		}

		rowEvent.Rows = append(rowEvent.Rows, row)
	}

	records, err := h.onRowsChange(rowEvent)
	if err != nil {
		return fmt.Errorf("unable to parse rows: %w", err)
	}

	for _, record := range records {
		select {
		case <-h.canalDoneC:
		case h.parsedRecordsC <- record:
		}
	}

	return nil
}

func (h *cdcEventHandler) String() string {
	return "cdcEventHandler"
}
