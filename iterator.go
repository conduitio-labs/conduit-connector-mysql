package mysql

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Iterator is an object that can iterate over a queue of records.
type Iterator interface {
	// Next takes and returns the next record from the queue. Next is allowed to
	// block until either a record is available or the context gets canceled.
	Next(context.Context) (sdk.Record, error)
	// Ack signals that a record at a specific position was successfully
	// processed.
	Ack(context.Context, sdk.Position) error
	// Teardown attempts to gracefully teardown the iterator.
	Teardown(context.Context) error
}
