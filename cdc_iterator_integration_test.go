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

func testCdcIterator(ctx context.Context, is *is.I) (Iterator, func()) {
	iterator, err := newCdcIterator(ctx, cdcIteratorConfig{
		SourceConfig: SourceConfig{
			Config: common.Config{
				Host:     "127.0.0.1",
				Port:     3306,
				User:     "root",
				Password: "meroxaadmin",
				Database: "meroxadb",
			},
			Tables: []string{"users"},
		},
		tableKeys: testTableKeys(),
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func testCdcIteratorAtPosition(
	ctx context.Context, is *is.I,
	position sdk.Position,
) (Iterator, func()) {
	iterator, err := newCdcIterator(ctx, cdcIteratorConfig{
		SourceConfig: SourceConfig{
			Config: common.Config{
				Host:     "127.0.0.1",
				Port:     3306,
				User:     "root",
				Password: "meroxaadmin",
				Database: "meroxadb",
			},
			Tables: []string{"users"},
		},
		tableKeys: testTableKeys(),
		position:  position,
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func TestCDCIterator_InsertAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	iterator, teardown := testCdcIterator(ctx, is)
	defer teardown()

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	makeKey := func(id int32) sdk.Data {
		return sdk.StructuredData{
			"id":     id,
			"table":  tableName("users"),
			"action": "insert",
		}
	}

	makePayload := func(id int32) sdk.Change {
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
		user    testutils.User
	}{
		{makeKey(1), makePayload(1), user1},
		{makeKey(2), makePayload(2), user2},
		{makeKey(3), makePayload(3), user3},
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
		isDataEqual(is, rec.Key, expected.key)

		isDataEqual(is, rec.Payload.After, expected.user.ToStructuredData())
	}
}

func TestCDCIterator_DeleteAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	iterator, teardown := testCdcIterator(ctx, is)
	defer teardown()

	userTable.Delete(is, db, user1)
	userTable.Delete(is, db, user2)
	userTable.Delete(is, db, user3)

	makeKey := func(id int32) sdk.Data {
		return sdk.StructuredData{
			"id":     id,
			"table":  tableName("users"),
			"action": "delete",
		}
	}

	testCases := []struct {
		key sdk.Data
	}{
		{makeKey(1)},
		{makeKey(2)},
		{makeKey(3)},
	}

	for _, expected := range testCases {
		rec, err := iterator.Next(ctx)
		is.NoErr(err)
		is.NoErr(iterator.Ack(ctx, rec.Position))

		is.Equal(rec.Operation, sdk.OperationDelete)
		is.Equal(rec.Metadata[keyAction], "delete")

		col, err := rec.Metadata.GetCollection()
		is.NoErr(err)
		is.Equal(col, "users")
		isDataEqual(is, rec.Key, expected.key)
	}
}

func TestCDCIterator_UpdateAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	iterator, teardown := testCdcIterator(ctx, is)
	defer teardown()

	user1Updated := userTable.Update(is, db, user1.Update())
	user2Updated := userTable.Update(is, db, user2.Update())
	user3Updated := userTable.Update(is, db, user3.Update())

	testCases := []struct {
		original, updated sdk.StructuredData
	}{
		{user1.ToStructuredData(), user1Updated.ToStructuredData()},
		{user2.ToStructuredData(), user2Updated.ToStructuredData()},
		{user3.ToStructuredData(), user3Updated.ToStructuredData()},
	}

	for _, expected := range testCases {

		rec, err := iterator.Next(ctx)
		is.NoErr(err)
		is.NoErr(iterator.Ack(ctx, rec.Position))

		is.Equal(rec.Operation, sdk.OperationUpdate)
		is.Equal(rec.Metadata[keyAction], "update")

		col, err := rec.Metadata.GetCollection()
		is.NoErr(err)
		is.Equal(col, "users")

		isDataEqual(is, rec.Payload.Before, expected.original)
		isDataEqual(is, rec.Payload.After, expected.updated)
	}
}

func TestCDCIterator_RestartOnPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	// start the iterator at the beginning

	iterator, teardown := testCdcIterator(ctx, is)

	// and trigger some insert actions

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")
	user4 := userTable.Insert(is, db, "user4")

	var latestPosition sdk.Position

	{ // read and ack 2 records
		rec1, err := iterator.Next(ctx)
		is.NoErr(err)
		is.NoErr(iterator.Ack(ctx, rec1.Position))

		assertUserInsert(is, user1, rec1)

		rec2, err := iterator.Next(ctx)
		is.NoErr(err)
		is.NoErr(iterator.Ack(ctx, rec2.Position))

		assertUserInsert(is, user2, rec2)

		teardown()

		latestPosition = rec2.Position
	}

	// then, try to read from the second record

	iterator, teardown = testCdcIteratorAtPosition(ctx, is, latestPosition)
	defer teardown()

	user5 := userTable.Insert(is, db, "user5")

	rec3, err := iterator.Next(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec3.Position))

	assertUserInsert(is, user3, rec3)

	rec4, err := iterator.Next(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec3.Position))

	assertUserInsert(is, user4, rec4)

	rec5, err := iterator.Next(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec3.Position))

	assertUserInsert(is, user5, rec5)
}

func assertUserInsert(is *is.I, user testutils.User, rec sdk.Record) {
	is.Equal(rec.Operation, sdk.OperationCreate)
	is.Equal(rec.Metadata[keyAction], "insert")

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")
	isDataEqual(is, rec.Key, sdk.StructuredData{
		"id":     user.ID,
		"table":  tableName("users"),
		"action": "insert",
	})

	isDataEqual(is, rec.Payload.After, user.ToStructuredData())
}

func isDataEqual(is *is.I, a, b sdk.Data) {
	if a == nil && b == nil {
		return
	}

	if a == nil || b == nil {
		is.Fail() // one of the data is nil
	}

	aS, aOK := a.(sdk.StructuredData)
	bS, bOK := b.(sdk.StructuredData)

	if aOK && bOK {
		for k, v := range aS {
			is.Equal(v, bS[k])
		}
	} else {
		is.Equal(string(a.Bytes()), string(b.Bytes()))
	}
}
