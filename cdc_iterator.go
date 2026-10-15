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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// ErrCDCStartPositionUnavailable is a stable, actionable error returned when the
// binlog file a CDC resume requires is no longer available on the server (e.g.
// purged by binlog_expire_logs_seconds). Before this fix, this failure mode
// stalled ReadN forever instead of surfacing an error; see the Decision section
// of docs/design-documents/20260724-snapshot-cdc-position-handoff.md.
var ErrCDCStartPositionUnavailable = errors.New("cdc start position unavailable")

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
}

type cdcIteratorConfig struct {
	db                  *sqlx.DB
	tables              []string
	mysqlConfig         *mysqldriver.Config
	tableKeys           common.TableKeys
	disableCanalLogging bool
	startPosition       *common.CdcPosition

	// coverage is stamped on every CDC record's position; see
	// common.TableCoverage.
	coverage *common.TableCoverage
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

// start begins binlog replication immediately from the iterator's start
// position (set by obtainStartPosition, or seeded from a persisted position).
func (c *cdcIterator) start(ctx context.Context) error {
	startPosition, err := c.getStartPosition()
	if err != nil {
		return fmt.Errorf("failed to get start position: %w", err)
	}

	tables := tablesFingerprint(c.config.tables)
	plan := planResume(startPosition, tables)

	eventHandler := newCdcEventHandler(
		ctx,
		c.canal,
		c.canalDoneC,
		c.parsedRecordsC,
		c.config.tableKeys,
		plan,
		tables,
		c.config.coverage,
	)

	// Set the handler before the goroutine starts: canal.Close reads it, so a
	// Close racing a not-yet-scheduled goroutine was a data race.
	c.canal.SetEventHandler(eventHandler)

	if plan.note != "" {
		sdk.Logger(ctx).Warn().
			Str("binlog_file", plan.from.Name).
			Uint32("binlog_pos", plan.from.Pos).
			Msg(plan.note)
	}

	go func() {
		// Invariant 3: read from a point at or before the saved event where
		// canal can decode it; the handler drops what was delivered already
		// (see planResume).
		c.canalRunErr = c.canal.RunFrom(plan.from.ToMysqlPos())
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

func (c *cdcIterator) Ack(context.Context, opencdc.Position) error {
	return nil
}

func (c *cdcIterator) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
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

// isSyncClosed reports whether err is go-mysql's benign "Sync was closed"
// signal — the expected result of canal.Close() interrupting RunFrom. errors.Is
// alone is insufficient: when Close lands during the initial "start sync
// replication" phase, go-mysql wraps ErrSyncClosed with errors.Errorf("... %v",
// err) (canal/sync.go), which formats the cause with %v and breaks the Unwrap
// chain, so only a string match catches it. The message is a stable sentinel in
// the pinned go-mysql v1.14.0 (replication.ErrSyncClosed = "Sync was closed").
//
// A Close that lands while the syncer is handing an event to the streamer
// fails that stream with a third form, a plain errors.New("sync is being
// closed...") with no sentinel (replication/binlogsyncer.go, onStream), which
// canal returns from RunFrom as is. It only happens when the syncer's context
// was cancelled, which only Close does, so it is the same benign stop.
func isSyncClosed(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, replication.ErrSyncClosed) ||
		strings.Contains(err.Error(), replication.ErrSyncClosed.Error()) ||
		strings.Contains(err.Error(), syncBeingClosedMsg)
}

// syncBeingClosedMsg is the text of go-mysql's unexported "closing" error; see
// isSyncClosed.
const syncBeingClosedMsg = "sync is being closed"

// wrapCanalRunErr turns canal.RunFrom's return value into a stable, actionable
// error. A graceful Teardown-triggered close is reported as such; any other
// error (most commonly a purged/expired binlog file) is reported as
// ErrCDCStartPositionUnavailable, naming the binlog file and position that could
// not be resumed from. Before this fix (mid-snapshot P0 resume,
// docs/design-documents/20260724-snapshot-cdc-position-handoff.md) this failure
// mode was never surfaced: ReadN blocked forever and the error was only drained
// (and discarded into the Teardown error, if any) once Teardown ran.
func (c *cdcIterator) wrapCanalRunErr() error {
	err := c.canalRunErr
	if err == nil {
		return errors.New("canal is closed")
	}
	if isSyncClosed(err) {
		return fmt.Errorf("canal is closed: %w", err)
	}

	// A handler halt is not a missing binlog; report it as itself.
	var skipped *SkippedColumnsError
	if errors.As(err, &skipped) {
		return skipped
	}
	var coded *Error
	if errors.As(err, &coded) {
		return coded
	}

	// canal rejects a PartialUpdateRowsEvent before the handler runs; it is
	// not a purged binlog either.
	if isPartialJSON(err) {
		pj := &PartialJSONError{Cause: err}
		if c.canal != nil {
			synced := c.canal.SyncedPosition()
			pj.BinlogFile, pj.AfterPos = synced.Name, synced.Pos
		} else if c.position != nil {
			pj.BinlogFile, pj.AfterPos = c.position.Name, c.position.Pos
		}
		return pj
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

func (c *cdcIterator) Teardown(ctx context.Context) error {
	close(c.canalDoneC)

	c.canal.Close()
	select {
	case <-ctx.Done():
		//nolint:wrapcheck // no need to wrap canceled error
		return ctx.Err()
	case <-c.canalRunDoneC:
		err := c.canalRunErr
		if isSyncClosed(err) {
			// Benign: canal.Close() interrupted RunFrom. Using error level might
			// be too much.
			sdk.Logger(ctx).Warn().Err(err).Msg("error found when closing mysql canal")
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to stop canal: %w", err)
		}
	}

	return nil
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
	coverage         *common.TableCoverage

	onRowsChange onRowChangeFn

	// logCtx carries the logger for canal's callbacks, which get no context.
	logCtx context.Context //nolint:containedctx // only for logging from canal's callbacks

	// The fields below are only used on canal's goroutine (OnRow, OnGTID and
	// OnPosSynced for events of the stream). canal.Close calls OnPosSynced
	// from the goroutine that tears down, with a nil header; that call
	// returns before touching them.

	// replayPoint is where canal could restart reading and decode every
	// event after it: the last position it synced with an offset (after a
	// commit, BEGIN, DDL or rotation), or the end of the last GTID event,
	// which is the start of the transaction after it. Stamped as ReplayFrom
	// on every record.
	replayPoint common.ReplicationEventPosition
	// compressedSeen counts the rows events without an offset (those inside
	// a compressed transaction) received since replayPoint was set.
	compressedSeen int
	// tables fingerprints the captured table set; see
	// common.CompressedEventRef.
	tables string

	// Resume state, from the resumePlan; see onRows.
	target           *common.ReplicationEventPosition
	targetCompressed *common.CompressedEventRef
	skipRows         int
	prevPosition     common.ReplicationEventPosition
}

func newCdcEventHandler(
	ctx context.Context,
	canal *canal.Canal,
	canalDoneC chan struct{},
	parsedRecordsC chan opencdc.Record,
	tablesPrimaryKeys common.TableKeys,
	plan resumePlan,
	tables string,
	coverage *common.TableCoverage,
) *cdcEventHandler {
	h := &cdcEventHandler{
		coverage:         coverage,
		canal:            canal,
		canalDoneC:       canalDoneC,
		parsedRecordsC:   parsedRecordsC,
		tablePrimaryKeys: tablesPrimaryKeys,
		replayPoint:      plan.from,
		tables:           tables,
		target:           plan.target,
		targetCompressed: plan.targetCompressed,
		skipRows:         plan.skipRows,
		prevPosition:     plan.prev,
	}
	h.logCtx = ctx
	h.onRowsChange = func(e rowEvent) ([]opencdc.Record, error) { return h.onRows(ctx, e) }

	return h
}

// tablesFingerprint identifies a captured table set, independent of order.
func tablesFingerprint(tables []string) string {
	sorted := slices.Clone(tables)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	sum := sha256.New()
	for _, t := range sorted {
		// Length-prefixed, so no two sets encode the same.
		_, _ = fmt.Fprintf(sum, "%d:%s", len(t), t)
	}
	return hex.EncodeToString(sum.Sum(nil))[:16]
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

// buildRecords turns the rows of one binlog event into records. firstIndex is
// the index, within the event, of e.Rows[0]: a resume in the middle of an event
// drops the rows before it (see handleSingleRowChange), and the positions must
// still name each row's place in the whole event.
func (h *cdcEventHandler) buildRecords(
	ctx context.Context,
	e rowEvent,
	prevPos common.ReplicationEventPosition,
	replayFrom common.ReplicationEventPosition,
	compressed *common.CompressedEventRef,
	firstIndex int,
) ([]opencdc.Record, error) {
	keySchema := newSchemaMapper()
	payloadSchema := newSchemaMapper()

	metadata, err := h.createMetadata(ctx, e, keySchema, payloadSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata: %w", err)
	}

	records := make([]opencdc.Record, 0, len(e.Rows))
	for i, row := range e.Rows {
		payloadAfter, err := h.buildPayload(ctx, payloadSchema, e, row.after)
		if err != nil {
			return nil, err
		}
		key := h.buildKey(ctx, e, payloadAfter, keySchema)

		var payloadBefore opencdc.StructuredData
		if row.before != nil {
			payloadBefore, err = h.buildPayload(ctx, payloadSchema, e, row.before)
			if err != nil {
				return nil, err
			}
		}

		pos := common.CdcPosition{
			ReplicationEventPosition: common.ReplicationEventPosition{
				Name: h.canal.SyncedPosition().Name,
				Pos:  e.Header.LogPos,
			},
			PrevPosition: &prevPos,
			ReplayFrom:   &replayFrom,
			Compressed:   compressed,
			// Invariant 2: the row's index in the whole event, so positions
			// keep increasing across a resume in the middle of the event and a
			// second resume skips exactly the rows already delivered.
			Index:         firstIndex + i,
			TableCoverage: h.coverage,
		}.ToSDKPosition()

		// Every record owns its metadata. The SDK middleware writes schema
		// subjects and versions into it per record, and its batching
		// goroutine does that while earlier records of the same event are
		// already in the caller's hands; a map shared between them is a data
		// race (a fatal concurrent map access at worst).
		recMeta := maps.Clone(metadata)

		var rec opencdc.Record
		switch e.Action {
		case canal.InsertAction:
			rec = sdk.Util.Source.NewRecordCreate(pos, recMeta, key, payloadAfter)
		case canal.UpdateAction:
			rec = sdk.Util.Source.NewRecordUpdate(pos, recMeta, key, payloadBefore, payloadAfter)
		case canal.DeleteAction:
			rec = sdk.Util.Source.NewRecordDelete(pos, recMeta, key, payloadAfter)
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

// buildPayload maps the values of one row image to the table's columns.
//
// Invariant 6: canal decodes an event with the table's schema as it is when
// the event is read, not as it was when the event was written. An event
// written before a column was added has fewer values than the table has
// columns now (this used to panic on the missing index, on every restart);
// one written before a column was dropped has more, and mapping the first
// values to the remaining columns shifted every value after a dropped middle
// column into the wrong column, silently. The same happens when the
// connector's user cannot see a column (column privileges, or a generated
// invisible primary key hidden by show_gipk_in_create_table_and_information_schema).
// Which column each value belongs to is unknown in all of these, so this
// halts with CodeRowColumnsMismatch instead of guessing. Generated (virtual or
// stored) and INVISIBLE columns are both logged and listed, so they match.
// A cached column list that is only out of date (canal skips DDL it cannot
// parse) is re-read in OnRow before this runs; see refreshStaleTable.
func (h *cdcEventHandler) buildPayload(
	ctx context.Context,
	payloadSchema *schemaMapper,
	e rowEvent,
	values []any,
) (opencdc.StructuredData, error) {
	columns := e.Table.Columns
	if len(values) != len(columns) {
		cause := "a column was added after the event was written"
		hidden := ""
		if len(values) > len(columns) {
			cause = "a column was dropped after the event was written, or the connector's user cannot see every column " +
				"(column privileges, or a generated invisible primary key hidden by " +
				"show_gipk_in_create_table_and_information_schema=OFF, which halts by design)"
			hidden = "If columns are hidden from the connector's user, make them visible first (grant SELECT on every " +
				"column; SET PERSIST show_gipk_in_create_table_and_information_schema = ON). "
		}
		return nil, newError(ErrRowColumnsMismatch, CodeRowColumnsMismatch, "tables",
			hidden+"Restart the pipeline first: the connector re-reads the table's columns, which resolves a column list "+
				"that was out of date or hidden. If it halts again on the same event, the event was written under another "+
				"schema: reset the pipeline's position to snapshot again (expect duplicates; see the README on recovery)",
			"a binlog event for table %s.%s in %s (event end offset %d) has %d values, but the table now has %d columns: %s",
			e.Table.Schema, e.Table.Name, h.canal.SyncedPosition().Name, e.Header.LogPos, len(values), len(columns), cause)
	}
	payload := opencdc.StructuredData{}
	for i, col := range columns {
		payload[col.Name] = payloadSchema.formatValue(ctx, col.Name, values[i])
	}
	return payload, nil
}

// resumePlan is how the CDC iterator restarts from a saved position: where
// canal starts reading, and which of the rows it reads again were delivered
// before.
type resumePlan struct {
	// from is where canal starts reading.
	from common.ReplicationEventPosition
	// target is the event of the saved position. Every event before it is
	// dropped, and the first skipRows rows of it. nil: deliver everything.
	target *common.ReplicationEventPosition
	// targetCompressed is set when target is inside a compressed transaction
	// and has no offset (target.Pos is 0): the target is then the
	// targetCompressed.Ordinal-th rows event without an offset after from.
	targetCompressed *common.CompressedEventRef
	skipRows         int
	// prev seeds PrevPosition for the first event handled.
	prev common.ReplicationEventPosition
	// note is logged when the plan reads or delivers more than it has to.
	note string
}

// planResume derives the resume plan from a saved CDC position. tables is the
// fingerprint of the table set captured now (tablesFingerprint).
//
// Invariant 3: canal can only decode a rows event if it read the Table_map
// events before it, so it must start at a point where canal itself would
// resume: a transaction boundary, a BEGIN, a rotation, or the start of a
// transaction after its GTID event. Positions written by this version carry
// that point (ReplayFrom).
//
// Events inside a compressed transaction (binlog_transaction_compression=ON)
// have no offset of their own: canal reports 0. Such a position names its
// event by counting from ReplayFrom (Compressed); it resumes exactly while the
// captured table set is the one it was counted under, and otherwise delivers
// the compressed transaction again from its start.
//
// Older positions carry only PrevPosition, the end of the previous captured
// rows event, which can lie in the middle of the saved event's statement (a
// large statement is logged as several rows events after one Table_map), and
// in positions written right after a resume in the middle of an event it is
// the event's own end (the bug fixed with this change). For those the binlog
// file is read from its start and the saved event found by its offset:
// slower, once, never wrong. An older position inside a compressed
// transaction has no offset to find, so that file is delivered again from its
// start.
func planResume(start common.CdcPosition, tables string) resumePlan {
	own := start.ReplicationEventPosition
	if start.PrevPosition == nil {
		// A fresh start (the server position, or the snapshot's P0): no
		// record was delivered from here yet.
		return resumePlan{from: own, prev: own}
	}
	target := own
	fileStart := common.ReplicationEventPosition{Name: own.Name, Pos: binlogFirstEventPos}

	// A ReplayFrom without an offset was written by an unreleased build of
	// this change, inside a compressed transaction; it is no place to start.
	if replay := start.ReplayFrom; replay != nil && replay.Pos >= binlogFirstEventPos {
		if own.Pos != 0 {
			return resumePlan{from: *replay, target: &target, skipRows: start.Index + 1, prev: *start.PrevPosition}
		}
		if ref := start.Compressed; ref != nil && ref.Ordinal > 0 && ref.Tables == tables {
			compressed := *ref
			return resumePlan{
				from: *replay, target: &target, targetCompressed: &compressed,
				skipRows: start.Index + 1, prev: *start.PrevPosition,
			}
		}
		return resumePlan{
			from: *replay, prev: *start.PrevPosition,
			note: "resuming inside a compressed binlog transaction whose position cannot be matched (the captured " +
				"tables changed since it was written); delivering that transaction again from its start (duplicates possible)",
		}
	}

	switch {
	case own.Pos == 0:
		return resumePlan{
			from: fileStart, prev: fileStart,
			note: "resuming from a CDC position written by an older version inside a compressed binlog transaction, " +
				"which has no offset to find again; delivering the binlog file again from its start (duplicates possible)",
		}
	case *start.PrevPosition == own:
		// Written by a version with the mid-event resume bug: Index counted
		// from the resume point, not from the start of the event, so the
		// delivered rows of the event are unknown. Deliver all of it again
		// (duplicates, never a gap).
		return resumePlan{
			from: fileStart, target: &target, skipRows: 0, prev: fileStart,
			note: "resuming from a CDC position written by an older version after a restart in the middle of a " +
				"binlog event; re-reading the binlog file from its start and delivering that event again in full (duplicates possible)",
		}
	default:
		return resumePlan{
			from: fileStart, target: &target, skipRows: start.Index + 1, prev: fileStart,
			note: "resuming from a CDC position written by an older version; re-reading the binlog file from its start to find the saved event",
		}
	}
}

// binlogFirstEventPos is the offset of the first event in a binlog file,
// after the 4-byte magic number.
const binlogFirstEventPos = 4

// onRows drops what a restart reads again but was delivered before, and
// builds the records of the rest. It runs on canal's goroutine.
func (h *cdcEventHandler) onRows(ctx context.Context, e rowEvent) ([]opencdc.Record, error) {
	event := common.ReplicationEventPosition{
		Name: h.canal.SyncedPosition().Name,
		Pos:  e.Header.LogPos,
	}
	inCompressed := e.Header.LogPos == 0
	var compressed *common.CompressedEventRef
	if inCompressed {
		compressed = &common.CompressedEventRef{Ordinal: h.compressedSeen, Tables: h.tables}
	}
	firstIndex := 0

	// Invariant 3: drop exactly what was delivered before the saved position.
	switch {
	case h.targetCompressed != nil:
		// canal started at the compressed transaction of the saved event, and
		// the table set is the one its ordinal was counted under, so the
		// events arrive in the same order and are counted the same.
		if !inCompressed {
			return nil, errResumeEventNotFound(
				"the saved CDC position names rows event %d of the compressed transaction at %s:%d, but an event at "+
					"offset %d came first", h.targetCompressed.Ordinal, h.replayPoint.Name, h.replayPoint.Pos, e.Header.LogPos)
		}
		if compressed.Ordinal < h.targetCompressed.Ordinal {
			h.prevPosition = event
			return nil, nil
		}
		// compressed.Ordinal == h.targetCompressed.Ordinal: the events are
		// counted one at a time, so the target is never passed over.
		if err := h.skipDelivered(&e); err != nil {
			return nil, err
		}
		firstIndex = h.skipRows
		h.target, h.targetCompressed = nil, nil
	case h.target != nil && inCompressed:
		// An event without an offset seen while the saved event (which has
		// one) is still ahead lies before it. h.passedTarget ends this once a
		// later offset shows up, so a saved event that never comes cannot
		// make compressed transactions after it disappear.
		h.prevPosition = event
		return nil, nil
	case h.target != nil:
		switch event.Compare(*h.target) {
		case -1:
			h.prevPosition = event
			return nil, nil
		case 0:
			if err := h.skipDelivered(&e); err != nil {
				return nil, err
			}
			firstIndex = h.skipRows
			h.target = nil
		default:
			// The saved event did not come (its table is no longer
			// captured, for example): everything from here is new.
			h.target = nil
		}
	}

	rows, err := h.buildRecords(ctx, e, h.prevPosition, h.replayPoint, compressed, firstIndex)

	h.prevPosition = event

	return rows, err
}

// skipDelivered drops the rows of the saved event that were delivered before
// the restart.
func (h *cdcEventHandler) skipDelivered(e *rowEvent) error {
	if h.skipRows > len(e.Rows) {
		// The saved index is past the end of its event: the position does not
		// describe this binlog. Delivering from here could skip rows or repeat
		// them, and which is unknown, so halt.
		return errResumeEventNotFound(
			"the saved CDC position points at row %d of the binlog event ending at %s:%d, which has %d rows",
			h.skipRows-1, h.canal.SyncedPosition().Name, e.Header.LogPos, len(e.Rows))
	}
	e.Rows = e.Rows[h.skipRows:]
	return nil
}

// passedTarget is called for every event with an offset that is not a rows
// event. While the saved event (with an offset) is ahead, an offset past it
// means it did not come, and the events from here on are new. While a saved
// event inside a compressed transaction is ahead, any offset means it was not
// found where the position says.
func (h *cdcEventHandler) passedTarget(at common.ReplicationEventPosition) error {
	switch {
	case h.targetCompressed != nil:
		return errResumeEventNotFound(
			"the saved CDC position names rows event %d of the compressed transaction at %s:%d, but the binlog "+
				"reached %s:%d before it", h.targetCompressed.Ordinal, h.replayPoint.Name, h.replayPoint.Pos, at.Name, at.Pos)
	case h.target != nil && at.Compare(*h.target) > 0:
		h.target = nil
	}
	return nil
}

// OnGTID moves the replay point to the end of a MySQL GTID event, which is the
// start of its transaction. That is the only place canal can restart from and
// still name an event inside a compressed transaction: the BEGIN and commit of
// such a transaction are inside it and carry no offset.
func (h *cdcEventHandler) OnGTID(header *replication.EventHeader, gtid mysql.BinlogGTIDEvent) error {
	if _, ok := gtid.(*replication.GTIDEvent); !ok || header == nil || header.LogPos == 0 {
		// MariaDB has no compressed transactions of this kind; its replay
		// point stays the last synced position.
		return nil
	}
	at := common.ReplicationEventPosition{Name: h.canal.SyncedPosition().Name, Pos: header.LogPos}
	if err := h.passedTarget(at); err != nil {
		return err
	}
	h.replayPoint, h.compressedSeen = at, 0
	return nil
}

// OnPosSynced moves the replay point to each position canal syncs with an
// offset. Inside a compressed transaction canal syncs offset 0, which is no
// place to restart from, and is skipped.
func (h *cdcEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, _ mysql.GTIDSet, _ bool) error {
	if header == nil {
		// Not an event of the stream: canal.Close calls this from the
		// goroutine that tears down. Return before touching any state.
		return nil
	}
	if pos.Pos == 0 {
		return nil
	}
	at := common.ReplicationEventPosition{Name: pos.Name, Pos: pos.Pos}
	if err := h.passedTarget(at); err != nil {
		return err
	}
	h.replayPoint, h.compressedSeen = at, 0
	return nil
}

func (h *cdcEventHandler) OnRow(e *canal.RowsEvent) error {
	// Count before anything can return: the count names compressed events on
	// a restart, and must match what a restart from replayPoint counts. Rows
	// from canal's dump have no header; the dump is disabled.
	if e.Header != nil && e.Header.LogPos == 0 {
		h.compressedSeen++
	}

	// A row image wider or narrower than the cached columns may only mean
	// the cache is stale; re-read it once before deciding to halt.
	h.refreshStaleTable(h.logCtx, e)

	// Invariant 6: halt before building any record if the row image left a
	// column out; see checkSkippedColumns.
	if err := checkSkippedColumns(e); err != nil {
		err.BinlogFile = h.canal.SyncedPosition().Name
		return err
	}

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
