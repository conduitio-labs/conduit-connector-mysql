package mysql

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/csync"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type cdcEventHandler struct{}

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
	fmt.Printf("Table: %s\n", e.Table.Name)
	fmt.Printf("Action: %s\n", e.Action)
	fmt.Printf("Rows: %v\n", e.Rows)
	return nil
}

func (h *cdcEventHandler) String() string {
	return "cdcEventHandler"
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

	go func() {
		// implement listening to canal events
		panic("unimplemented")
	}()

	return iterator, nil
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
			return sdk.Record{}, fmt.Errorf("iterator teared down")
		}

		c.acks.Add(1)
		return c.buildRecord(data), nil
	}
}

// Teardown implements Iterator.
func (c *cdcIterator) Teardown(context.Context) error {
	close(c.data)
	return nil
}

func (c *cdcIterator) buildRecord(d *canal.RowsEvent) sdk.Record {
	panic("unimplemented")
}
