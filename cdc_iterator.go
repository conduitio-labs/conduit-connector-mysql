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
		Name: masterPos.Name,
		Pos:  masterPos.Pos,
	}

	return nil
}

func (c *cdcIterator) start(ctx context.Context) error {
	startPosition, err := c.getStartPosition()
	if err != nil {
		return fmt.Errorf("failed to get start position: %w", err)
	}
	eventHandler := newCdcEventHandler(c.canal, c.canalDoneC, c.rowsEventsC)

	go c.readCDCEvents(ctx)

	go func() {
		c.canal.SetEventHandler(eventHandler)
		pos := startPosition.ToMysqlPos()

		c.canalRunErrC <- c.canal.RunFrom(pos)
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

	return common.CdcPosition{Name: masterPos.Name, Pos: masterPos.Pos}, nil
}

func (c *cdcIterator) Ack(context.Context, opencdc.Position) error {
	return nil
}

func (c *cdcIterator) readCDCEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.canalDoneC:
			return
		case data := <-c.rowsEventsC:
			recs, err := c.buildRecords(ctx, data)
			if err != nil {
				c.parsedRecordsErrC <- fmt.Errorf("unable to build record: %w", err)
			}

			for _, r := range recs {
				c.parsedRecordsC <- r
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
	avroCols := make([]*avroNamedType, len(e.Table.Columns))
	for i, col := range e.Table.Columns {
		avroCol, err := mysqlSchemaToAvroCol(col)
		if err != nil {
			return nil, fmt.Errorf("failed to parse avro cols: %w", err)
		}
		avroCols[i] = avroCol
	}

	tableName := e.Table.Name

	payloadSubver, err := payloadSchema.createPayloadSchema(ctx, tableName, avroCols)
	if err != nil {
		return nil, fmt.Errorf("failed to create cdc payload schema for table %s: %w", tableName, err)
	}

	metadata := opencdc.Metadata{}
	metadata.SetCollection(e.Table.Name)
	metadata.SetCreatedAt(time.Unix(int64(e.Header.Timestamp), 0).UTC())
	metadata[common.ServerIDKey] = strconv.FormatUint(uint64(e.Header.ServerID), 10)

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
	keyCol := c.config.tableSortCols[e.Table.Name]

	if keyCol == "" {
		keyVal := fmt.Sprintf("%s_%d", e.binlogName, e.Header.LogPos)
		return opencdc.RawData(keyVal)
	}

	keyVal := keySchema.formatValue(ctx, keyCol, payload[keyCol])
	return opencdc.StructuredData{keyCol: keyVal}
}

func (c *cdcIterator) buildRecords(ctx context.Context, e rowEvent) ([]opencdc.Record, error) {
	var records []opencdc.Record

	keySchema := newSchemaMapper()
	payloadSchema := newSchemaMapper()

	metadata, err := c.createMetadata(ctx, e, keySchema, payloadSchema)
	if err != nil {
		return records, fmt.Errorf("failed to create metadata: %w", err)
	}

	pos := common.CdcPosition{
		Name: e.binlogName,
		Pos:  e.Header.LogPos,
	}.ToSDKPosition()

	for i := 0; i < len(e.Rows); i++ {
		payload := c.buildPayload(ctx, payloadSchema, e.Table.Columns, e.Rows[i])
		key := c.buildKey(ctx, e, payload, keySchema)

		switch e.Action {
		case canal.InsertAction:
			records = append(records,
				sdk.Util.Source.NewRecordCreate(pos, metadata, key, payload),
			)

		case canal.UpdateAction:
			// updated rows are going in pairs:
			// [ row_before, row_after, row_before, row_after ]
			i++
			if i >= len(e.Rows) {
				return records,
					fmt.Errorf("invalid number of rows in CDC event of type %q", canal.UpdateAction)
			}

			payloadBefore := payload
			payloadAfter := c.buildPayload(ctx, payloadSchema, e.Table.Columns, e.Rows[i])
			key = c.buildKey(ctx, e, payloadAfter, keySchema)

			records = append(records,
				sdk.Util.Source.NewRecordUpdate(pos, metadata, key, payloadBefore, payloadAfter),
			)

		case canal.DeleteAction:
			records = append(records,
				sdk.Util.Source.NewRecordDelete(pos, metadata, key, payload),
			)

		default:
			return records, fmt.Errorf("unknown action type: %v", e.Action)
		}
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

type rowEvent struct {
	*canal.RowsEvent
	binlogName string
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
	rowEvent := rowEvent{e, binlogName}
	select {
	case <-h.canalDoneC:
	case h.rowsEventsC <- rowEvent:
	}

	return nil
}

func (h *cdcEventHandler) String() string {
	return "cdcEventHandler"
}
