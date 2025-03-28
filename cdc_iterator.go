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

	rowsEventsC  chan rowEvent
	canalRunErrC chan error
	canalDoneC   chan struct{}

	parsedRecordsC    chan opencdc.Record
	parsedRecordsErrC chan error
}

type cdcIteratorConfig struct {
	db                  *sqlx.DB
	tables              []string
	mysqlConfig         *mysqldriver.Config
	tableSortCols       map[string]string
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
		config:            config,
		canal:             canal,
		position:          config.startPosition,
		rowsEventsC:       make(chan rowEvent),
		canalRunErrC:      make(chan error),
		canalDoneC:        make(chan struct{}),
		parsedRecordsC:    make(chan opencdc.Record),
		parsedRecordsErrC: make(chan error),
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
	eventHandler := newCdcEventHandler(c.canal, c.canalDoneC, c.rowsEventsC)

	go c.readCDCEvents(ctx, startPosition)

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

func pushToChan[T any](
	ctx context.Context,
	canalDoneC <-chan struct{},
	dataChan chan<- T,
	data T,
) {
	select {
	case <-ctx.Done():
	case <-canalDoneC:
	case dataChan <- data:
	}
}

func (c *cdcIterator) readCDCEvents(ctx context.Context, startPosition common.CdcPosition) {
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

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.canalDoneC:
			return
		case e := <-c.rowsEventsC:
			recs, err := c.buildRecords(ctx, e, prevPosition)
			if err != nil {
				pushToChan(ctx, c.canalDoneC, c.parsedRecordsErrC,
					fmt.Errorf("unable to build record: %w", err),
				)
			}

			if len(recs) < requiredOffset {
				// should be impossible
				sdk.Logger(ctx).Error().
					Any("position", startPosition).
					Msg("unexpected number of rows in the event: some records could be lost")
			}

			for i := requiredOffset; i < len(recs); i++ {
				pushToChan(ctx, c.canalDoneC, c.parsedRecordsC, recs[i])
			}

			// Only a part of the first event could be skipped.
			requiredOffset = 0

			prevPosition = common.ReplicationEventPosition{
				Name: e.binlogName,
				Pos:  e.header.LogPos,
			}
		}
	}
}

func (c *cdcIterator) Read(ctx context.Context) (rec opencdc.Record, _ error) {
	select {
	//nolint:wrapcheck // no need to wrap canceled error
	case <-ctx.Done():
		return rec, ctx.Err()
	case <-c.canalDoneC:
		return rec, fmt.Errorf("canal is closed")
	case err := <-c.parsedRecordsErrC:
		return rec, err
	case rec := <-c.parsedRecordsC:
		return rec, nil
	}
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

func (c *cdcIterator) createMetadata(
	ctx context.Context,
	e rowEvent,
	keySchema *schemaMapper,
	payloadSchema *schemaMapper,
) (opencdc.Metadata, error) {
	avroCols := make([]*avroNamedType, len(e.table.Columns))
	for i, col := range e.table.Columns {
		avroCol, err := mysqlSchemaToAvroCol(col)
		if err != nil {
			return nil, fmt.Errorf("failed to parse avro cols: %w", err)
		}
		avroCols[i] = avroCol
	}

	tableName := e.table.Name

	payloadSubver, err := payloadSchema.createPayloadSchema(ctx, tableName, avroCols)
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc payload schema for table %s: %w", tableName, err)
	}

	metadata := opencdc.Metadata{}
	metadata.SetCollection(e.table.Name)
	metadata.SetCreatedAt(time.Unix(int64(e.header.Timestamp), 0).UTC())
	metadata[common.ServerIDKey] = strconv.FormatUint(uint64(e.header.ServerID), 10)

	metadata.SetPayloadSchemaSubject(payloadSubver.subject)
	metadata.SetPayloadSchemaVersion(payloadSubver.version)

	if keyCol := c.config.tableSortCols[tableName]; keyCol != "" {
		keyColType, found := findKeyColType(avroCols, keyCol)
		if !found {
			return nil, fmt.Errorf("failed to find key schema column type for table %s", tableName)
		}

		keySubver, err := keySchema.createKeySchema(ctx, tableName, keyColType)
		if err != nil {
			return nil, fmt.Errorf("failed to create key schema for table %s: %w", tableName, err)
		}

		metadata.SetKeySchemaSubject(keySubver.subject)
		metadata.SetKeySchemaVersion(keySubver.version)
	}

	return metadata, nil
}

func (c *cdcIterator) buildKey(
	ctx context.Context,
	e rowEvent,
	payload opencdc.StructuredData,
	keySchema *schemaMapper,
) opencdc.Data {
	keyCol := c.config.tableSortCols[e.table.Name]

	if keyCol == "" {
		keyVal := fmt.Sprintf("%s_%d", e.binlogName, e.header.LogPos)
		return opencdc.RawData(keyVal)
	}

	keyVal := keySchema.formatValue(ctx, keyCol, payload[keyCol])
	return opencdc.StructuredData{keyCol: keyVal}
}

func (c *cdcIterator) buildRecords(
	ctx context.Context,
	e rowEvent,
	prevPos common.ReplicationEventPosition,
) ([]opencdc.Record, error) {
	keySchema := newSchemaMapper()
	payloadSchema := newSchemaMapper()

	metadata, err := c.createMetadata(ctx, e, keySchema, payloadSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata: %w", err)
	}

	records := make([]opencdc.Record, 0, len(e.rows))
	for i, row := range e.rows {
		payloadAfter := c.buildPayload(ctx, payloadSchema, e.table.Columns, row.after)
		key := c.buildKey(ctx, e, payloadAfter, keySchema)

		var payloadBefore opencdc.StructuredData
		if row.before != nil {
			payloadBefore = c.buildPayload(ctx, payloadSchema, e.table.Columns, row.before)
		}

		pos := common.CdcPosition{
			ReplicationEventPosition: common.ReplicationEventPosition{
				Name: e.binlogName,
				Pos:  e.header.LogPos,
			},
			PrevPosition: &prevPos,
			Index:        i,
		}.ToSDKPosition()

		var rec opencdc.Record
		switch e.action {
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

func (c *cdcIterator) buildPayload(
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

type replicationEventRow struct {
	before []any
	after  []any
}

type rowEvent struct {
	binlogName string
	action     string
	table      *schema.Table
	rows       []replicationEventRow
	header     *replication.EventHeader
}

type cdcEventHandler struct {
	canal.DummyEventHandler
	canal *canal.Canal

	canalDoneC  chan struct{}
	rowsEventsC chan rowEvent
}

var _ canal.EventHandler = new(cdcEventHandler)

func newCdcEventHandler(
	canal *canal.Canal,
	canalDoneC chan struct{},
	rowsEventsC chan rowEvent,
) *cdcEventHandler {
	return &cdcEventHandler{
		canal:       canal,
		canalDoneC:  canalDoneC,
		rowsEventsC: rowsEventsC,
	}
}

func (h *cdcEventHandler) OnRow(e *canal.RowsEvent) error {
	binlogName := h.canal.SyncedPosition().Name

	rowEvent := rowEvent{
		binlogName: binlogName,
		table:      e.Table,
		header:     e.Header,
		action:     e.Action,
		rows:       make([]replicationEventRow, 0, len(e.Rows)),
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

		rowEvent.rows = append(rowEvent.rows, row)
	}

	select {
	case <-h.canalDoneC:
	case h.rowsEventsC <- rowEvent:
	}

	return nil
}

func (h *cdcEventHandler) String() string {
	return "cdcEventHandler"
}
