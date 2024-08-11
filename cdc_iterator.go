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
	"strconv"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type cdcIterator struct {
	canal       *canal.Canal
	rowsEventsC chan rowEvent

	canalRunErrC chan error
	canalDoneC   chan struct{}

	config cdcIteratorConfig
}

type cdcIteratorConfig struct {
	tables         []string
	mysqlConfig    *mysqldriver.Config
	position       *common.CdcPosition
	TableKeys      common.TableKeys
	disableLogging bool
}

func newCdcIterator(ctx context.Context, config cdcIteratorConfig) (common.Iterator, error) {
	c, err := common.NewCanal(common.CanalConfig{
		Config:         config.mysqlConfig,
		Tables:         config.tables,
		DisableLogging: config.disableLogging,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create canal: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("created canal")

	rowsEventsC := make(chan rowEvent)
	canalDoneC := make(chan struct{})

	iterator := &cdcIterator{
		canal:        c,
		rowsEventsC:  rowsEventsC,
		canalRunErrC: make(chan error),
		canalDoneC:   canalDoneC,
		config:       config,
	}

	startPosition, err := iterator.getStartPosition(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get start position: %w", err)
	}
	eventHandler := newCdcEventHandler(c, canalDoneC, rowsEventsC)

	go func() {
		iterator.canal.SetEventHandler(eventHandler)
		pos := startPosition.ToMysqlPos()
		iterator.canalRunErrC <- iterator.canal.RunFrom(pos)
	}()

	return iterator, nil
}

func (c *cdcIterator) getStartPosition(config cdcIteratorConfig) (common.CdcPosition, error) {
	if config.position != nil {
		return *config.position, nil
	}

	var cdcPosition common.CdcPosition
	masterPos, err := c.canal.GetMasterPos()
	if err != nil {
		return cdcPosition, fmt.Errorf("failed to get master position: %w", err)
	}

	return common.CdcPosition{
		Name: masterPos.Name,
		Pos:  masterPos.Pos,
	}, nil
}

func (c *cdcIterator) Ack(context.Context, opencdc.Position) error {
	return nil
}

func (c *cdcIterator) Next(ctx context.Context) (rec opencdc.Record, err error) {
	select {
	//nolint:wrapcheck // no need to wrap canceled error
	case <-ctx.Done():
		return rec, ctx.Err()
	case <-c.canalDoneC:
		return rec, fmt.Errorf("canal is closed")
	case data := <-c.rowsEventsC:
		rec, err := c.buildRecord(data)
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
		if err != nil {
			return fmt.Errorf("failed to stop canal: %w", err)
		}
	}

	return nil
}

func buildPayload(columns []schema.TableColumn, rows []any) opencdc.StructuredData {
	payload := opencdc.StructuredData{}
	for i, col := range columns {
		payload[col.Name] = common.FormatValue(rows[i])
	}
	return payload
}

func (c *cdcIterator) buildRecord(e rowEvent) (opencdc.Record, error) {
	pos := common.CdcPosition{
		Name: e.binlogName,
		Pos:  e.Header.LogPos,
	}

	metadata := opencdc.Metadata{}
	metadata.SetCollection(e.Table.Name)
	createdAt := time.Unix(int64(e.Header.Timestamp), 0).UTC()
	metadata.SetCreatedAt(createdAt)
	metadata[common.ServerIDKey] = strconv.FormatUint(uint64(e.Header.ServerID), 10)

	switch e.Action {
	case canal.InsertAction:
		position := pos.ToSDKPosition()
		payload := buildPayload(e.Table.Columns, e.Rows[0])

		table := common.TableName(e.Table.Name)
		primaryKey := c.config.TableKeys[table]

		key, err := buildRecordKey(primaryKey, payload)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordCreate(position, metadata, key, payload), nil
	case canal.DeleteAction:
		position := pos.ToSDKPosition()

		payload := buildPayload(e.Table.Columns, e.Rows[0])

		table := common.TableName(e.Table.Name)
		primaryKey := c.config.TableKeys[table]

		key, err := buildRecordKey(primaryKey, payload)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordDelete(position, metadata, key, nil), nil
	case canal.UpdateAction:
		position := pos.ToSDKPosition()
		before := buildPayload(e.Table.Columns, e.Rows[0])
		after := buildPayload(e.Table.Columns, e.Rows[1])

		table := common.TableName(e.Table.Name)
		primaryKey := c.config.TableKeys[table]

		key, err := buildRecordKey(primaryKey, before)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordUpdate(position, metadata, key, before, after), nil
	}

	return opencdc.Record{}, fmt.Errorf("unknown row event action: %s", e.Action)
}

func buildRecordKey(
	primaryKey common.PrimaryKeyName,
	payload opencdc.StructuredData,
) (opencdc.StructuredData, error) {
	val, ok := payload[string(primaryKey)]
	if !ok {
		return nil, fmt.Errorf("key %s not found in payload", primaryKey)
	}

	return opencdc.StructuredData{string(primaryKey): val}, nil
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
