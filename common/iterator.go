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

package common

import (
	"context"

	"github.com/conduitio/conduit-commons/opencdc"
)

// Iterator is an object that can iterate over a queue of records.
type Iterator interface {
	// Read takes and returns the next record from the queue. Read is allowed to
	// block until either a record is available or the context gets canceled.
	Next(context.Context) (opencdc.Record, error)
	// Ack signals that a record at a specific position was successfully
	// processed.
	Ack(context.Context, opencdc.Position) error
	// Teardown attempts to gracefully teardown the iterator.
	Teardown(context.Context) error
}
