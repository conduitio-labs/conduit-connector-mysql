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
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

func testCdcIterator(ctx context.Context, t *testing.T, is *is.I) (common.Iterator, func()) {
	db := testutils.Connection(t)

	config, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)

	iterator, err := newCdcIterator(ctx, cdcIteratorConfig{
		mysqlConfig:         config,
		tables:              []string{"users"},
		tableKeys:           testutils.TableKeys,
		db:                  db,
		disableCanalLogging: true,
	})
	is.NoErr(err)

	is.NoErr(iterator.obtainStartPosition())
	is.NoErr(iterator.start())

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func testCdcIteratorAtPosition(
	ctx context.Context, t *testing.T, is *is.I,
	sdkPos opencdc.Position,
) (common.Iterator, func()) {
	db := testutils.Connection(t)

	config, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)

	pos, err := common.ParseSDKPosition(sdkPos)
	is.NoErr(err)

	is.Equal(pos.Kind, common.PositionTypeCDC)

	iterator, err := newCdcIterator(ctx, cdcIteratorConfig{
		db:                  db,
		mysqlConfig:         config,
		tables:              []string{"users"},
		tableKeys:           testutils.TableKeys,
		startPosition:       pos.CdcPosition,
		disableCanalLogging: true,
	})
	is.NoErr(err)

	is.NoErr(iterator.start())

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func TestCDCIterator_InsertAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	userTable.Recreate(is, db)

	iterator, teardown := testCdcIterator(ctx, t, is)
	defer teardown()

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	testutils.ReadAndAssertCreate(ctx, is, iterator, user1)
	testutils.ReadAndAssertCreate(ctx, is, iterator, user2)
	testutils.ReadAndAssertCreate(ctx, is, iterator, user3)
}

func TestCDCIterator_DeleteAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	iterator, teardown := testCdcIterator(ctx, t, is)
	defer teardown()

	userTable.Delete(is, db, user1)
	userTable.Delete(is, db, user2)
	userTable.Delete(is, db, user3)

	testutils.ReadAndAssertDelete(ctx, is, iterator, user1)
	testutils.ReadAndAssertDelete(ctx, is, iterator, user2)
	testutils.ReadAndAssertDelete(ctx, is, iterator, user3)
}

func TestCDCIterator_UpdateAction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	iterator, teardown := testCdcIterator(ctx, t, is)
	defer teardown()

	user1Updated := userTable.Update(is, db, user1.Update())
	user2Updated := userTable.Update(is, db, user2.Update())
	user3Updated := userTable.Update(is, db, user3.Update())

	testutils.ReadAndAssertUpdate(ctx, is, iterator, user1, user1Updated)
	testutils.ReadAndAssertUpdate(ctx, is, iterator, user2, user2Updated)
	testutils.ReadAndAssertUpdate(ctx, is, iterator, user3, user3Updated)
}

func TestCDCIterator_RestartOnPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	userTable.Recreate(is, db)

	// start the iterator at the beginning

	iterator, teardown := testCdcIterator(ctx, t, is)

	// and trigger some insert actions

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")
	user4 := userTable.Insert(is, db, "user4")

	var latestPosition opencdc.Position

	{ // read and ack 2 records
		testutils.ReadAndAssertCreate(ctx, is, iterator, user1)
		rec := testutils.ReadAndAssertCreate(ctx, is, iterator, user2)
		teardown()

		latestPosition = rec.Position
	}

	// then, try to read from the second record

	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, latestPosition)
	defer teardown()

	user5 := userTable.Insert(is, db, "user5")

	testutils.ReadAndAssertCreate(ctx, is, iterator, user3)
	testutils.ReadAndAssertCreate(ctx, is, iterator, user4)
	testutils.ReadAndAssertCreate(ctx, is, iterator, user5)
}
