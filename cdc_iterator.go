package mysql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/conduitio/conduit-commons/csync"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
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
	canal *canal.Canal
	acks  *csync.WaitGroup
	data  chan *canal.RowsEvent
}

type cdcIteratorConfig struct {
	database string
	tables   []string
}

func newCdcIterator(ctx context.Context, config SourceConfig) (Iterator, error) {
	c, err := newCanal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create canal: %w", err)
	}

	iterator := &cdcIterator{
		canal: c,
		acks:  &csync.WaitGroup{},
		data:  make(chan *canal.RowsEvent),
	}

	now, err := iterator.canal.GetMasterPos()
	if err != nil {
		return nil, fmt.Errorf("failed to get master position: %w", err)
	}

	go iterator.runCanal(ctx, now)

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
	c.canal.Close()
	close(c.data)
	return nil
}

func (c *cdcIterator) buildRecord(e *canal.RowsEvent) (sdk.Record, error) {
	pos, err := c.canal.GetMasterPos()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("failed to get master position from buildRecord: %w", err)
	}

	position := cdcPosition{pos}.toSDKPosition()

	return sdk.Record{
		Operation: 0,
		Position:  position,
		Metadata: sdk.Metadata{
			"table":  e.Table.Name,
			"action": string(e.Action),
		},

		Key: buildRecordKey(e),
		Payload: sdk.Change{
			Before: buildRecordPayload(e, true),
			After:  buildRecordPayload(e, false),
		},
	}, nil
}

func buildRecordKey(e *canal.RowsEvent) sdk.Data {
	if len(e.Rows) > 0 && len(e.Rows[0]) > 0 {
		return sdk.StructuredData{"id": e.Rows[0][0]}
	}
	return nil
}

func buildRecordPayload(e *canal.RowsEvent, isBefore bool) sdk.Data {
	var row []interface{}
	if isBefore {
		if e.Action == canal.UpdateAction && len(e.Rows) > 1 {
			row = e.Rows[0] // For updates, first row is before state
		} else if e.Action == canal.DeleteAction {
			row = e.Rows[0] // For deletes, we have the before state
		}
	} else {
		if e.Action == canal.UpdateAction && len(e.Rows) > 1 {
			row = e.Rows[1] // For updates, second row is after state
		} else if e.Action == canal.InsertAction {
			row = e.Rows[0] // For inserts, we have the after state
		}
	}

	if row == nil {
		return nil
	}

	payload := sdk.StructuredData{}
	for i, col := range e.Table.Columns {
		if i < len(row) {
			payload[col.Name] = row[i]
		}
	}
	return payload
}

type cdcEventHandler struct {
	ctx  context.Context
	data chan *canal.RowsEvent
}

func (h *cdcEventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (h *cdcEventHandler) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	return nil
}

func (h *cdcEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *cdcEventHandler) OnRotate(header *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	return nil
}

func (h *cdcEventHandler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	return nil
}

func (h *cdcEventHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	return nil
}

func (h *cdcEventHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
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
