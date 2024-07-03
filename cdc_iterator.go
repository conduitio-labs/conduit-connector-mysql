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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/conduitio/conduit-commons/csync"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"gopkg.in/tomb.v2"
)

type cdcPosition struct {
	mysql.Position
}

func (p cdcPosition) toSDKPosition() sdk.Position {
	v, err := json.Marshal(p)
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

type cdcIterator struct {
	canal     *canal.Canal
	acks      *csync.WaitGroup
	data      chan *canal.RowsEvent
	tableKeys tableKeys

	// canalTomb is a tomb that simplifies handling canal run errors
	canalTomb *tomb.Tomb
}

func newCdcIterator(ctx context.Context, config SourceConfig) (Iterator, error) {
	c, err := newCanal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create canal: %w", err)
	}

	db, err := newSqlxDB(config.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql: %w", err)
	}

	iterator := &cdcIterator{
		canal:     c,
		acks:      &csync.WaitGroup{},
		data:      make(chan *canal.RowsEvent),
		tableKeys: map[tableName]primaryKeyName{},
		canalTomb: &tomb.Tomb{},
	}

	iterator.tableKeys = make(tableKeys)

	for _, table := range config.Tables {
		primaryKey, err := getPrimaryKey(db, config.Database, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key for table %q: %w", table, err)
		}

		iterator.tableKeys[tableName(table)] = primaryKey
	}

	now, err := iterator.canal.GetMasterPos()
	if err != nil {
		return nil, fmt.Errorf("failed to get master position: %w", err)
	}

	iterator.canalTomb.Go(func() error {
		ctx := iterator.canalTomb.Context(ctx)
		return iterator.runCanal(ctx, now)
	})

	go func() {
		<-iterator.canalTomb.Dead()
		close(iterator.data)
	}()

	return iterator, nil
}

func (c *cdcIterator) runCanal(ctx context.Context, startPos mysql.Position) error {
	errChan := make(chan error, 1)
	go func() {
		errChan <- c.canal.RunFrom(startPos)
	}()

	handler := &cdcEventHandler{
		ctx:  ctx,
		data: c.data,
	}
	c.canal.SetEventHandler(handler)

	select {
	case <-ctx.Done():
		c.canal.Close()
		return fmt.Errorf("context cancelled from runCanal: %w", ctx.Err())
	case err := <-errChan:
		return fmt.Errorf("failed to run canal: %w", err)
	}
}

func (c *cdcIterator) Ack(context.Context, sdk.Position) error {
	c.acks.Done()
	return nil
}

func (c *cdcIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, fmt.Errorf("context cancelled: %w", ctx.Err())
	case data, ok := <-c.data:
		if !ok { // closed
			if err := c.canalTomb.Err(); err != nil {
				return sdk.Record{}, fmt.Errorf("canal exited unexpectedly: %w", err)
			}
			if err := c.acks.Wait(ctx); err != nil {
				return sdk.Record{}, fmt.Errorf("failed to wait for acks: %w", err)
			}
			return sdk.Record{}, fmt.Errorf("cdc iterator teared down")
		}

		rec, err := c.buildRecord(data)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("failed to build record: %w", err)
		}

		c.acks.Add(1)
		return rec, nil
	}
}

func (c *cdcIterator) Teardown(context.Context) error {
	if c.canalTomb != nil {
		c.canalTomb.Kill(errors.New("tearing down snapshot iterator"))
	}

	return nil
}

func buildPayload(columns []schema.TableColumn, rows []any) sdk.StructuredData {
	payload := sdk.StructuredData{}
	for i, col := range columns {
		if i < len(rows) {
			payload[col.Name] = rows[i]
		}
	}
	return payload
}

var keyAction = "mysql.action"

func (c *cdcIterator) buildRecord(e *canal.RowsEvent) (sdk.Record, error) {
	pos, err := c.canal.GetMasterPos()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed to get master position from buildRecord: %w", err)
	}

	switch e.Action {
	case canal.InsertAction:
		position := cdcPosition{pos}.toSDKPosition()
		metadata := sdk.Metadata{
			keyAction: e.Action,
		}

		payload := buildPayload(e.Table.Columns, e.Rows[0])

		metadata.SetCollection(e.Table.Name)
		table := tableName(e.Table.Name)
		primaryKey := c.tableKeys[table]

		key, err := buildRecordKey(primaryKey, table, e.Action, payload)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordCreate(position, metadata, key, payload), nil
	case canal.DeleteAction:
		position := cdcPosition{pos}.toSDKPosition()
		metadata := sdk.Metadata{
			keyAction: e.Action,
		}

		payload := buildPayload(e.Table.Columns, e.Rows[0])

		metadata.SetCollection(e.Table.Name)
		table := tableName(e.Table.Name)
		primaryKey := c.tableKeys[table]

		key, err := buildRecordKey(primaryKey, table, e.Action, payload)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("failed to build record key: %w", err)
		}

		return sdk.Util.Source.NewRecordDelete(position, metadata, key), nil

	case canal.UpdateAction:
		// TODO
	}

	return sdk.Record{}, fmt.Errorf("unknown row event action: %s", e.Action)
}

func buildRecordKey(
	primaryKey primaryKeyName, table tableName,
	action string, payload sdk.StructuredData,
) (sdk.Data, error) {
	val, ok := payload[string(primaryKey)]
	if !ok {
		return nil, fmt.Errorf("key %s not found in payload", primaryKey)
	}

	return sdk.StructuredData{
		string(primaryKey): val,
		"table":            table,
		"action":           action,
	}, nil
}

type cdcEventHandler struct {
	//nolint:containedctx // ctx is used to allow to timeout the OnRow event
	ctx  context.Context
	data chan *canal.RowsEvent
}

func (h *cdcEventHandler) OnDDL(_ *replication.EventHeader, _ mysql.Position, _ *replication.QueryEvent) error {
	return nil
}

func (h *cdcEventHandler) OnGTID(_ *replication.EventHeader, _ mysql.BinlogGTIDEvent) error {
	return nil
}

func (h *cdcEventHandler) OnPosSynced(_ *replication.EventHeader, _ mysql.Position, _ mysql.GTIDSet, _ bool) error {
	return nil
}

func (h *cdcEventHandler) OnRotate(_ *replication.EventHeader, _ *replication.RotateEvent) error {
	return nil
}

func (h *cdcEventHandler) OnRowsQueryEvent(_ *replication.RowsQueryEvent) error {
	return nil
}

func (h *cdcEventHandler) OnTableChanged(_ *replication.EventHeader, _ string, _ string) error {
	return nil
}

func (h *cdcEventHandler) OnXID(_ *replication.EventHeader, _ mysql.Position) error {
	return nil
}

func (h *cdcEventHandler) OnRow(e *canal.RowsEvent) error {
	select {
	case <-h.ctx.Done():
		return fmt.Errorf("context cancelled from OnRow: %w", h.ctx.Err())
	case h.data <- e:
	}

	return nil
}

func (h *cdcEventHandler) String() string {
	return "cdcEventHandler"
}
