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
		config:       config,
		canal:        canal,
		position:     config.startPosition,
		rowsEventsC:  make(chan rowEvent),
		canalRunErrC: make(chan error),
		canalDoneC:   make(chan struct{}),
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

func (c *cdcIterator) start() error {
	startPosition, err := c.getStartPosition()
	if err != nil {
		return fmt.Errorf("failed to get start position: %w", err)
	}
	eventHandler := newCdcEventHandler(c.canal, c.canalDoneC, c.rowsEventsC)

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

func (c *cdcIterator) Read(ctx context.Context) (rec opencdc.Record, err error) {
	select {
	//nolint:wrapcheck // no need to wrap canceled error
	case <-ctx.Done():
		return rec, ctx.Err()
	case <-c.canalDoneC:
		return rec, fmt.Errorf("canal is closed")
	case data := <-c.rowsEventsC:
		rec, err := c.buildRecord(ctx, data)
		if err != nil {
			return rec, fmt.Errorf("failed to build record: %w", err)
		}

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

func (c *cdcIterator) buildRecord(ctx context.Context, e rowEvent) (opencdc.Record, error) {
	pos := common.CdcPosition{
		Name: e.binlogName,
		Pos:  e.Header.LogPos,
	}.ToSDKPosition()

	payloadAvroCols := make([]*avroNamedType, len(e.Table.Columns))
	for i, col := range e.Table.Columns {
		avroCol, err := mysqlSchemaToAvroCol(col)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to parse avro cols: %w", err)
		}
		payloadAvroCols[i] = avroCol
	}

	payloadSchema, keySchema := newSchemaMapper(), newSchemaMapper()
	tableName := e.Table.Name

	payloadSubver, err := payloadSchema.createPayloadSchema(ctx, tableName, payloadAvroCols)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to create cdc payload schema for table %s: %w", tableName, err)
	}

	metadata := opencdc.Metadata{}
	metadata.SetCollection(e.Table.Name)
	metadata.SetCreatedAt(time.Unix(int64(e.Header.Timestamp), 0).UTC())
	metadata[common.ServerIDKey] = strconv.FormatUint(uint64(e.Header.ServerID), 10)

	metadata.SetPayloadSchemaSubject(payloadSubver.subject)
	metadata.SetPayloadSchemaVersion(payloadSubver.version)

	var payloadBefore, payloadAfter, payload opencdc.StructuredData
	payloadBefore = c.buildPayload(ctx, payloadSchema, e.Table.Columns, e.Rows[0])
	if len(e.Rows) > 1 {
		payloadAfter = c.buildPayload(ctx, payloadSchema, e.Table.Columns, e.Rows[1])
	}

	// payload really is just an alias, but makes buildRecord easier to understand.
	payload = payloadBefore

	keyCols := c.config.primaryKeys[tableName]
	var key opencdc.Data

	if len(keyCols) == 0 {
		keyVal := fmt.Sprintf("%s_%d", e.binlogName, e.Header.LogPos)
		key = opencdc.RawData(keyVal)
	} else {
		var keyAvroCols []*avroNamedType
		for _, keyCol := range keyCols {
			keyColType, found := findKeyColType(payloadAvroCols, keyCol)
			if !found {
				return opencdc.Record{}, fmt.Errorf("failed to find key schema column type for table %s", tableName)
			}
			keyAvroCols = append(keyAvroCols, keyColType)
		}

		keySubver, err := keySchema.createKeySchema(ctx, tableName, keyAvroCols)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to create key schema for table %s: %w", tableName, err)
		}

		structuredKey := opencdc.StructuredData{}
		for _, keyCol := range keyCols {
			keyVal := payload[keyCol]
			// In update events, the key has to be the value after the update,
			// otherwise we could use a stale value.
			if e.Action == canal.UpdateAction {
				keyVal = payloadAfter[keyCol]
			}
			structuredKey[keyCol] = keySchema.formatValue(ctx, keyCol, keyVal)
		}

		key = structuredKey

		metadata.SetKeySchemaSubject(keySubver.subject)
		metadata.SetKeySchemaVersion(keySubver.version)
	}

	switch e.Action {
	case canal.InsertAction:
		return sdk.Util.Source.NewRecordCreate(pos, metadata, key, payload), nil
	case canal.UpdateAction:
		return sdk.Util.Source.NewRecordUpdate(pos, metadata, key, payloadBefore, payloadAfter), nil
	case canal.DeleteAction:
		return sdk.Util.Source.NewRecordDelete(pos, metadata, key, payload), nil
	default:
		return opencdc.Record{}, fmt.Errorf("unknown action type: %v", e.Action)
	}
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
