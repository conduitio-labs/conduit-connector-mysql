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
	"reflect"
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
		TableKeys: testutils.TableKeys,
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
		TableKeys: testutils.TableKeys,
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

	readAndAssertInsert(ctx, is, iterator, user1)
	readAndAssertInsert(ctx, is, iterator, user2)
	readAndAssertInsert(ctx, is, iterator, user3)
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

	readAndAssertDelete(ctx, is, iterator, user1)
	readAndAssertDelete(ctx, is, iterator, user2)
	readAndAssertDelete(ctx, is, iterator, user3)
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

	readAndAssertUpdate(ctx, is, iterator, user1, user1Updated)
	readAndAssertUpdate(ctx, is, iterator, user2, user2Updated)
	readAndAssertUpdate(ctx, is, iterator, user3, user3Updated)
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
		readAndAssertInsert(ctx, is, iterator, user1)
		rec := readAndAssertInsert(ctx, is, iterator, user2)
		teardown()

		latestPosition = rec.Position
	}

	// then, try to read from the second record

	iterator, teardown = testCdcIteratorAtPosition(ctx, is, latestPosition)
	defer teardown()

	user5 := userTable.Insert(is, db, "user5")

	readAndAssertInsert(ctx, is, iterator, user3)
	readAndAssertInsert(ctx, is, iterator, user4)
	readAndAssertInsert(ctx, is, iterator, user5)
}

func readAndAssertInsert(
	ctx context.Context, is *is.I,
	iterator Iterator, user testutils.User,
) sdk.Record {
	rec, err := iterator.Next(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, sdk.OperationCreate)

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	isDataEqual(is, rec.Key, sdk.StructuredData{
		"id":     user.ID,
		"table":  "users",
		"action": "insert",
	})

	isDataEqual(is, rec.Payload.After, user.ToStructuredData())

	return rec
}

func readAndAssertUpdate(
	ctx context.Context, is *is.I,
	iterator Iterator, prev, next testutils.User,
) {
	rec, err := iterator.Next(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, sdk.OperationUpdate)
	is.Equal(rec.Metadata[keyAction], "update")

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	isDataEqual(is, rec.Payload.Before, prev.ToStructuredData())
	isDataEqual(is, rec.Payload.After, next.ToStructuredData())
}

func readAndAssertDelete(
	ctx context.Context, is *is.I,
	iterator Iterator, user testutils.User,
) {
	rec, err := iterator.Next(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, sdk.OperationDelete)
	is.Equal(rec.Metadata[keyAction], "delete")

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	isDataEqual(is, rec.Key, sdk.StructuredData{
		"id":     user.ID,
		"table":  "users",
		"action": "delete",
	})
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
		equal, err := JSONBytesEqual(a.Bytes(), b.Bytes())
		is.NoErr(err)
		is.True(equal)
	}
}

// JSONBytesEqual compares the JSON in two byte slices.
func JSONBytesEqual(a, b []byte) (bool, error) {
	var j, j2 interface{}
	if err := json.Unmarshal(a, &j); err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, &j2); err != nil {
		return false, err
	}

	return reflect.DeepEqual(j2, j), nil
}
