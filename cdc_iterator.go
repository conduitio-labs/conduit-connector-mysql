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
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/csync"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type cdcIterator struct {
	canal *canal.Canal
	acks  *csync.WaitGroup
	data  chan *canal.RowsEvent

	canalRunErrC chan error
	canalDoneC   chan struct{}

	config cdcIteratorConfig
}

type cdcIteratorConfig struct {
	tables      []string
	mysqlConfig *mysqldriver.Config
	position    sdk.Position
	TableKeys   common.TableKeys
}

func newCdcIterator(ctx context.Context, config cdcIteratorConfig) (common.Iterator, error) {
	c, err := common.NewCanal(config.mysqlConfig, config.tables)
	if err != nil {
		return nil, fmt.Errorf("failed to create canal: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("created canal")

	iterator := &cdcIterator{
		canal:        c,
		acks:         &csync.WaitGroup{},
		data:         make(chan *canal.RowsEvent),
		canalRunErrC: make(chan error),
		canalDoneC:   make(chan struct{}),
		config:       config,
	}

	startPosition, err := iterator.getStartPosition(config)
	if err != nil {
		return nil, fmt.Errorf("failed to get start position: %w", err)
	}

	go func() {
		sdk.Logger(ctx).Info().Msg("running canal")
		iterator.canalRunErrC <- iterator.runCanal(startPosition)
	}()

	return iterator, nil
}

func (c *cdcIterator) getStartPosition(config cdcIteratorConfig) (mysql.Position, error) {
	if config.position != nil {
		pos, err := common.ParseSDKPosition(config.position)
		if err != nil {
			return mysql.Position{}, fmt.Errorf("failed to parse position: %w", err)
		}
		if pos.Kind == common.PositionTypeSnapshot {
			return mysql.Position{}, fmt.Errorf("invalid position type: %s", pos.Kind)
		}

		return pos.CdcPosition.Position, nil
	}

	masterPos, err := c.canal.GetMasterPos()
	if err != nil {
		return mysql.Position{}, fmt.Errorf("failed to get master position: %w", err)
	}

	return masterPos, nil
}

func (c *cdcIterator) runCanal(startPos mysql.Position) error {
	handler := &cdcEventHandler{
		canalDoneC: c.canalDoneC,
		data:       c.data,
	}
	c.canal.SetEventHandler(handler)

	return c.canal.RunFrom(startPos)
}

func (c *cdcIterator) Ack(context.Context, sdk.Position) error {
	c.acks.Done()
	return nil
}

func (c *cdcIterator) Next(ctx context.Context) (rec sdk.Record, err error) {
	select {
	case <-ctx.Done():
		return rec, fmt.Errorf(
			"context cancelled from cdc iterator next call: %w", ctx.Err(),
		)
	case <-c.canalDoneC:
		return rec, fmt.Errorf("canal closed")
	case data := <-c.data:
		rec, err := c.buildRecord(data)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("failed to build record: %w", err)
		}

		c.acks.Add(1)
		return rec, nil
	}
}

func (c *cdcIterator) Teardown(ctx context.Context) error {
	close(c.canalDoneC)

	c.canal.Close()
	if err := <-c.canalRunErrC; err != nil {
		return fmt.Errorf("failed to stop canal: %w", err)
	}

	if err := c.acks.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for cdc acks: %w", err)
	}

	return nil
}

func buildPayload(columns []schema.TableColumn, rows []any) sdk.StructuredData {
	payload := sdk.StructuredData{}
	for i, col := range columns {
		val := rows[i]
		if s, ok := val.(string); ok {
			// I don't know why exactly, but "github.com/go-mysql-org/go-mysql/canal"
			// returns a string for timestamp columns without timezone. This is a hack
			// to format the string back into an UTC string.
			// TODO: investigate this further. Is this a bug in canal?
			val = tryParseCanalStrDate(s)
		}

		payload[col.Name] = common.FormatValue(val)
	}
	return payload
}

func tryParseCanalStrDate(s string) string {
	parsed, err := time.Parse(time.DateTime, s)
	if err != nil {
		return s
	}

	valCopyInUTC := time.Date(
		parsed.Year(), parsed.Month(), parsed.Day(),
		parsed.Hour(), parsed.Minute(), parsed.Second(),
		parsed.Nanosecond(),
		time.Now().Location(),
	).UTC()

	return valCopyInUTC.Format(time.RFC3339)
}

func (c *cdcIterator) buildRecord(e *canal.RowsEvent) (sdk.Record, error) {
	pos, err := c.canal.GetMasterPos()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed to get master position from buildRecord: %w", err)
	}

	metadata := sdk.Metadata{"mysql.action": e.Action}
	metadata.SetCollection(e.Table.Name)

	pos.Pos = e.Header.LogPos

	switch e.Action {
	case canal.InsertAction:
		position := common.CdcPosition{Position: pos}.ToSDKPosition()
		payload := buildPayload(e.Table.Columns, e.Rows[0])

		table := common.TableName(e.Table.Name)
		primaryKey := c.config.TableKeys[table]

		key, err := buildRecordKey(primaryKey, table, e.Action, payload)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordCreate(position, metadata, key, payload), nil
	case canal.DeleteAction:
		position := common.CdcPosition{Position: pos}.ToSDKPosition()

		payload := buildPayload(e.Table.Columns, e.Rows[0])

		table := common.TableName(e.Table.Name)
		primaryKey := c.config.TableKeys[table]

		key, err := buildRecordKey(primaryKey, table, e.Action, payload)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordDelete(position, metadata, key), nil
	case canal.UpdateAction:
		position := common.CdcPosition{Position: pos}.ToSDKPosition()
		before := buildPayload(e.Table.Columns, e.Rows[0])
		after := buildPayload(e.Table.Columns, e.Rows[1])

		table := common.TableName(e.Table.Name)
		primaryKey := c.config.TableKeys[table]

		key, err := buildRecordKey(primaryKey, table, e.Action, before)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordUpdate(position, metadata, key, before, after), nil
	}

	return sdk.Record{}, fmt.Errorf("unknown row event action: %s", e.Action)
}

func buildRecordKey(
	primaryKey common.PrimaryKeyName, table common.TableName,
	action string, payload sdk.StructuredData,
) (sdk.StructuredData, error) {
	val, ok := payload[string(primaryKey)]
	if !ok {
		return nil, fmt.Errorf("key %s not found in payload", primaryKey)
	}

	return sdk.StructuredData{
		string(primaryKey): val,
		"table":            string(table),
		"action":           action,
	}, nil
}

type cdcEventHandler struct {
	// We only want the OnRow event, this allows us to ignore all other event methods
	canal.DummyEventHandler

	canalDoneC chan struct{}
	data       chan *canal.RowsEvent
}

func (h *cdcEventHandler) OnRow(e *canal.RowsEvent) error {
	select {
	case <-h.canalDoneC:
	case h.data <- e:
	}

	return nil
}

func (h *cdcEventHandler) String() string {
	return "cdcEventHandler"
}
