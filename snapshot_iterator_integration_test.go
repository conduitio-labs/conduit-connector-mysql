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
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

var userTable testutils.UsersTable

func testSnapshotIterator(ctx context.Context, is *is.I) (Iterator, func()) {
	iterator, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		tableKeys: testutils.TableKeys,
		db:        testutils.Connection(is),
		database:  "meroxadb",
		tables:    []string{"users"},
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func testSnapshotIteratorAtPosition(
	ctx context.Context, is *is.I,
	position snapshotPosition,
) (Iterator, func()) {
	iterator, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		tableKeys:     testutils.TableKeys,
		db:            testutils.Connection(is),
		startPosition: position,
		database:      "meroxadb",
		tables:        []string{"users"},
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func TestSnapshotIterator_EmptyTable(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	it, cleanup := testSnapshotIterator(ctx, is)
	defer cleanup()

	_, err := it.Next(ctx)
	if errors.Is(err, ErrSnapshotIteratorDone) {
		return
	}
	is.NoErr(err)
}

func TestSnapshotIterator_WithData(t *testing.T) {
	ctx := testutils.TestContext(t)

	is := is.New(t)

	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	var users []testutils.User
	for i := 0; i < 100; i++ {
		user := userTable.Insert(is, db, fmt.Sprintf("user-%v", i))
		users = append(users, user)
	}

	iterator, cleanup := testSnapshotIterator(ctx, is)
	defer cleanup()

	for i := 0; i < 100; i++ {
		readAndAssertSnapshot(ctx, is, iterator, users[i])
	}

	_, err := iterator.Next(ctx)
	is.True(errors.Is(err, ErrSnapshotIteratorDone))
}

func TestSnapshotIterator_SmallFetchSize(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	var users []testutils.User
	for i := 0; i < 100; i++ {
		user := userTable.Insert(is, db, fmt.Sprintf("user-%v", i))
		users = append(users, user)
	}

	iterator, cleanup := testSnapshotIterator(ctx, is)
	defer cleanup()

	for i := 0; i < 100; i++ {
		readAndAssertSnapshot(ctx, is, iterator, users[i])
	}

	_, err := iterator.Next(ctx)
	is.True(errors.Is(err, ErrSnapshotIteratorDone))
}

func TestSnapshotIterator_RestartOnPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(is)

	userTable.Recreate(is, db)
	var users []testutils.User
	for i := 0; i < 100; i++ {
		user := userTable.Insert(is, db, fmt.Sprintf("user-%v", i))
		users = append(users, user)
	}

	var recs []sdk.Record
	var breakPosition snapshotPosition
	{
		it, cleanup := testSnapshotIterator(ctx, is)
		defer cleanup()

		for i := 0; i < 10; i++ {
			rec, err := it.Next(ctx)
			if errors.Is(err, ErrSnapshotIteratorDone) {
				break
			}
			is.NoErr(err)

			recs = append(recs, rec)

			err = it.Ack(ctx, rec.Position)
			is.NoErr(err)
		}

		pos, err := parseSDKPosition(recs[len(recs)-1].Position)
		is.NoErr(err)
		is.Equal(pos.Kind, positionTypeSnapshot)

		breakPosition = *pos.SnapshotPosition
		is.NoErr(err)
	}

	// read the remaining 90 records

	it, cleanup := testSnapshotIteratorAtPosition(ctx, is, breakPosition)
	defer cleanup()

	for {
		rec, err := it.Next(ctx)
		if errors.Is(err, ErrSnapshotIteratorDone) {
			break
		}
		is.NoErr(err)

		recs = append(recs, rec)

		err = it.Ack(ctx, rec.Position)
		is.NoErr(err)
	}

	is.Equal(len(recs), 100)
	for i, rec := range recs {
		assertUserSnapshot(is, users[i], rec)
	}
}

func readAndAssertSnapshot(
	ctx context.Context, is *is.I,
	iterator Iterator, user testutils.User,
) {
	rec, err := iterator.Next(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, sdk.OperationSnapshot)

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	isDataEqual(is, rec.Key, sdk.StructuredData{
		"table": common.TableName("users"),
		"key":   "id",
		"value": user.ID,
	})

	isDataEqual(is, rec.Payload.After, user.ToStructuredData())
}

func assertUserSnapshot(is *is.I, user testutils.User, rec sdk.Record) {
	is.Equal(rec.Operation, sdk.OperationSnapshot)

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	isDataEqual(is, rec.Key, sdk.StructuredData{
		"key":   "id",
		"value": user.ID,
		"table": common.TableName("users"),
	})

	isDataEqual(is, rec.Payload.After, user.ToStructuredData())
}
