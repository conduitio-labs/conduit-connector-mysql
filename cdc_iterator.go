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

	return common.CdcPosition{
		Name: masterPos.Name,
		Pos:  masterPos.Pos,
	}, nil
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
		if errors.Is(err, replication.ErrSyncClosed) {
			return nil
		} else if err != nil {
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

		sortCol := c.config.tableSortCols[e.Table.Name]

		key, err := buildRecordKey(sortCol, payload)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordCreate(position, metadata, key, payload), nil
	case canal.DeleteAction:
		position := pos.ToSDKPosition()

		payload := buildPayload(e.Table.Columns, e.Rows[0])

		sortCol := c.config.tableSortCols[e.Table.Name]

		key, err := buildRecordKey(sortCol, payload)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordDelete(position, metadata, key, nil), nil
	case canal.UpdateAction:
		position := pos.ToSDKPosition()
		before := buildPayload(e.Table.Columns, e.Rows[0])
		after := buildPayload(e.Table.Columns, e.Rows[1])

		sortCol := c.config.tableSortCols[e.Table.Name]

		key, err := buildRecordKey(sortCol, before)
		if err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordUpdate(position, metadata, key, before, after), nil
	}

	return opencdc.Record{}, fmt.Errorf("unknown row event action: %s", e.Action)
}

func buildRecordKey(
	primaryKey string,
	payload opencdc.StructuredData,
) (opencdc.StructuredData, error) {
	val, ok := payload[primaryKey]
	if !ok {
		return nil, fmt.Errorf("key %s not found in payload", primaryKey)
	}

	return opencdc.StructuredData{primaryKey: val}, nil
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
