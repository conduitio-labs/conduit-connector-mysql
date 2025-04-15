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

type cdcIterator struct {
	config   cdcIteratorConfig
	canal    *canal.Canal
	position *common.CdcPosition

	canalDoneC     chan struct{}
	canalRunErrC   chan error
	parsedRecordsC chan opencdc.Record
}

type cdcIteratorConfig struct {
	db                  *sqlx.DB
	tables              []string
	mysqlConfig         *mysqldriver.Config
	primaryKeys         map[string]common.PrimaryKeys
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
		canalRunErrC:   make(chan error),
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

func (c *cdcIterator) start(ctx context.Context) error {
	startPosition, err := c.getStartPosition()
	if err != nil {
		return fmt.Errorf("failed to get start position: %w", err)
	}

	eventHandler := newCdcEventHandler(
		ctx,
		c.canal,
		c.canalDoneC,
		c.parsedRecordsC,
		c.config.primaryKeys,
		startPosition,
	)

	go func() {
		c.canal.SetEventHandler(eventHandler)

		// We need to run canal from Previous position to be sure
		// we didn't lose any record from multi-row mysql replication
		// event.
		pos := startPosition.ReplicationEventPosition
		if startPosition.PrevPosition != nil {
			pos = *startPosition.PrevPosition
		}

		c.canalRunErrC <- c.canal.RunFrom(pos.ToMysqlPos())
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
		return nil, ctx.Err()
	case <-c.canalDoneC:
		return nil, fmt.Errorf("canal is closed")
	case rec := <-c.parsedRecordsC:
		recs = append(recs, rec)
	}

	// try getting the remaining (n-1) records without blocking
	for len(recs) < n {
		select {
		case rec := <-c.parsedRecordsC:
			recs = append(recs, rec)
		case <-ctx.Done():
			return recs, ctx.Err()
		case <-c.canalDoneC:
			return recs, fmt.Errorf("canal is closed")
		default:
			// No more records currently available
			return recs, nil
		}
	}

	return recs, nil
}

func (c *cdcIterator) Teardown(ctx context.Context) error {
	close(c.canalDoneC)

	c.canal.Close()
	select {
	case <-ctx.Done():
		//nolint:wrapcheck // no need to wrap canceled error
		return ctx.Err()
	case err := <-c.canalRunErrC:
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

	tablePrimaryKeys map[string]common.PrimaryKeys

	onRowsChange onRowChangeFn
}

func newCdcEventHandler(
	ctx context.Context,
	canal *canal.Canal,
	canalDoneC chan struct{},
	parsedRecordsC chan opencdc.Record,
	tablesPrimaryKeys map[string]common.PrimaryKeys,
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
