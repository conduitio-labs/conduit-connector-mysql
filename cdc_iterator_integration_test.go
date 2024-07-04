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

	user1 := testTables.InsertUser(is, db, "user1")
	user2 := testTables.InsertUser(is, db, "user2")
	user3 := testTables.InsertUser(is, db, "user3")

	makeKey := func(id int32) sdk.Data {
		return sdk.StructuredData{
			"id":     id,
			"table":  tableName("users"),
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

		// is.Equal(payload["id"], expected.user.ID)
		// is.Equal(payload["username"], expected.user.Username)
		// is.Equal(payload["email"], expected.user.Email)
	}
}

func TestCDCIterator_DeleteAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.TestConnection(is)

	testTables.Drop(is, db)
	testTables.Create(is, db)

	user1 := testTables.InsertUser(is, db, "user1")
	user2 := testTables.InsertUser(is, db, "user2")
	user3 := testTables.InsertUser(is, db, "user3")

	iterator := testCdcIterator(ctx, is)

	testTables.DeleteUser(is, db, user1.ID)
	testTables.DeleteUser(is, db, user2.ID)
	testTables.DeleteUser(is, db, user3.ID)

	makeKey := func(id int) sdk.Data {
		return sdk.StructuredData{
			"id":     id,
			"table":  "users",
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

	db := testutils.TestConnection(is)

	testTables.Drop(is, db)
	testTables.Create(is, db)

	insertUser := func(username string) (testutils.User, testutils.User) {
		user := testTables.InsertUser(is, db, username)
		updated := user
		updated.Username = username + "-updated"
		updated.Email = username + "-updated@example.com"
		return user, updated
	}

	user1, updateUser1 := insertUser("user1")
	user2, updateUser2 := insertUser("user2")
	user3, updateUser3 := insertUser("user3")

	iterator := testCdcIterator(ctx, is)

	{ // update users to trigger update action
		testTables.UpdateUser(is, db, updateUser1)
		testTables.UpdateUser(is, db, updateUser2)
		testTables.UpdateUser(is, db, updateUser3)
	}

	testCases := []struct {
		original, updated sdk.StructuredData
	}{
		{user1.ToStructuredData(), updateUser1.ToStructuredData()},
		{user2.ToStructuredData(), updateUser2.ToStructuredData()},
		{user3.ToStructuredData(), updateUser3.ToStructuredData()},
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

func isChangeEqual(is *is.I, a, b sdk.Change) {
	isDataEqual(is, a.Before, b.Before)
	isDataEqual(is, a.After, b.After)
}
