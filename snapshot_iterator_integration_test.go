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
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

func testSnapshotIterator(ctx context.Context, t *testing.T, is *is.I) (common.Iterator, func()) {
	db := testutils.Connection(t)

	serverID, err := common.GetServerID(ctx, db)
	is.NoErr(err)

	canal := testutils.NewCanal(ctx, is)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tableKeys: testutils.TableKeys,
		db:        db,
		database:  "meroxadb",
		tables:    []string{"users"},
		serverID:  serverID,
	})
	is.NoErr(err)

	is.NoErr(iterator.setupWorkers(ctx))
	iterator.start(ctx)

	return iterator, func() {
		is.NoErr(db.Close())
		canal.Close()
		is.NoErr(iterator.Teardown(ctx))
	}
}

func testSnapshotIteratorAtPosition(
	ctx context.Context, t *testing.T, is *is.I,
	sdkPos opencdc.Position,
) (common.Iterator, func()) {
	db := testutils.Connection(t)

	serverID, err := common.GetServerID(ctx, db)
	is.NoErr(err)

	canal := testutils.NewCanal(ctx, is)

	pos, err := common.ParseSDKPosition(sdkPos)
	is.NoErr(err)

	is.Equal(pos.Kind, common.PositionTypeSnapshot)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tableKeys:     testutils.TableKeys,
		db:            db,
		startPosition: pos.SnapshotPosition,
		database:      "meroxadb",
		tables:        []string{"users"},
		serverID:      serverID,
	})
	is.NoErr(err)

	is.NoErr(iterator.setupWorkers(ctx))
	iterator.start(ctx)

	return iterator, func() {
		is.NoErr(db.Close())
		canal.Close()
		is.NoErr(iterator.Teardown(ctx))
	}
}

func TestSnapshotIterator_EmptyTable(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	it, cleanup := testSnapshotIterator(ctx, t, is)
	defer cleanup()

	_, err := it.Next(ctx)
	if !errors.Is(err, ErrSnapshotIteratorDone) {
		is.NoErr(err)
	}
}

func TestSnapshotIterator_WithData(t *testing.T) {
	ctx := testutils.TestContext(t)

	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	var users []testutils.User
	for i := 0; i < 100; i++ {
		user := testutils.InsertUser(is, db, i)
		users = append(users, user)
	}

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	iterator, cleanup := testSnapshotIterator(ctx, t, is)
	defer cleanup()

	for i := 0; i < 100; i++ {
		testutils.ReadAndAssertSnapshot(ctx, is, iterator, users[i])
	}

	_, err := iterator.Next(ctx)
	is.True(errors.Is(err, ErrSnapshotIteratorDone))
}

func TestSnapshotIterator_SmallFetchSize(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	var users []testutils.User
	for i := 0; i < 100; i++ {
		user := testutils.InsertUser(is, db, i)
		users = append(users, user)
	}

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	iterator, cleanup := testSnapshotIterator(ctx, t, is)
	defer cleanup()

	for i := 0; i < 100; i++ {
		testutils.ReadAndAssertSnapshot(ctx, is, iterator, users[i])
	}

	_, err := iterator.Next(ctx)
	is.True(errors.Is(err, ErrSnapshotIteratorDone))
}

func TestSnapshotIterator_RestartOnPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)
	var users []testutils.User
	for i := 1; i <= 100; i++ {
		user := testutils.InsertUser(is, db, i)
		users = append(users, user)
	}

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	var recs []opencdc.Record
	var breakPosition opencdc.Position
	{
		it, cleanup := testSnapshotIterator(ctx, t, is)

		for i := 1; i <= 10; i++ {
			rec, err := it.Next(ctx)
			if errors.Is(err, ErrSnapshotIteratorDone) {
				err = it.Ack(ctx, rec.Position)
				is.NoErr(err)
				break
			}
			is.NoErr(err)

			recs = append(recs, rec)

			err = it.Ack(ctx, rec.Position)
			is.NoErr(err)
			breakPosition = rec.Position
		}

		// not deferring the call so that logs are easier to understand
		cleanup()
	}

	// read the remaining 90 records

	it, cleanup := testSnapshotIteratorAtPosition(ctx, t, is, breakPosition)
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
		testutils.AssertUserSnapshot(is, users[i], rec)
	}
}
