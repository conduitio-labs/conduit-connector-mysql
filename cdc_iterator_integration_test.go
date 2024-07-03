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
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func testCdcIterator(ctx context.Context, is *is.I) Iterator {
	iterator, err := newCdcIterator(ctx, SourceConfig{
		Config: common.Config{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "meroxaadmin",
			Database: "meroxadb",
		},
		Tables: []string{"users"},
	})
	is.NoErr(err)

	return iterator
}

func TestCDCIterator_InsertAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.TestConnection(is)

	testTables.Drop(is, db)
	testTables.Create(is, db)

	iterator := testCdcIterator(ctx, is)

	testTables.InsertUser(is, db, "user1")
	testTables.InsertUser(is, db, "user2")
	testTables.InsertUser(is, db, "user3")

	makeKey := func(id int) sdk.Data {
		return sdk.StructuredData{
			"id":     id,
			"table":  "users",
			"action": "insert",
		}
	}

	makePayload := func(id int) sdk.Change {
		return sdk.Change{
			Before: nil,
			After: sdk.StructuredData{
				"id":       id,
				"username": fmt.Sprintf("user%d", id),
				"email":    fmt.Sprintf("user%d@example.com", id),
			},
		}
	}

	testCases := []struct {
		key     sdk.Data
		payload sdk.Change
	}{
		{makeKey(1), makePayload(1)},
		{makeKey(2), makePayload(2)},
		{makeKey(3), makePayload(3)},
	}

	for _, expected := range testCases {
		rec, err := iterator.Next(ctx)
		is.NoErr(err)
		is.NoErr(iterator.Ack(ctx, rec.Position))

		is.Equal(rec.Operation, sdk.OperationCreate)
		is.Equal(rec.Metadata[keyAction], "insert")

		col, err := rec.Metadata.GetCollection()
		is.NoErr(err)
		is.Equal(col, "users")
		dataEqual(is, rec.Key, expected.key)

		// It is complex to mock created_at and not really worth it, considering
		// that there's no special logic to test to handle dates from the
		// connector side. We can just delete this part
		delete(rec.Payload.After.(sdk.StructuredData), "created_at")
		changeEqual(is, rec.Payload, expected.payload)
	}
}

func dataEqual(is *is.I, a, b sdk.Data) {
	if a == nil && b == nil {
		return
	}

	if a == nil || b == nil {
		is.Fail() // one of the data is nil
	}

	is.Equal(string(a.Bytes()), string(b.Bytes()))
}

func changeEqual(is *is.I, a, b sdk.Change) {
	dataEqual(is, a.Before, b.Before)
	dataEqual(is, a.After, b.After)
}
