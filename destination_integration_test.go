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
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func testDestination(ctx context.Context, is *is.I) (sdk.Destination, func()) {
	destination := &Destination{}
	err := destination.Configure(ctx, config.Config{
		common.DestinationConfigUrl:   testutils.DSN,
		common.DestinationConfigTable: "users",
		common.DestinationConfigKey:   "id",
	})
	is.NoErr(err)

	is.NoErr(destination.Open(ctx))

	return destination, func() { is.NoErr(destination.Teardown(ctx)) }
}

func TestDestination_OperationSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.Connection(t)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	src, cleanSrc := testSource(ctx, is)
	defer cleanSrc()

	rec1 := testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user1)
	rec2 := testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user2)
	rec3 := testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user3)

	// clean table to assert snapshots were written
	userTable.Recreate(is, db)

	written, err := dest.Write(ctx, []opencdc.Record{rec1, rec2, rec3})
	is.NoErr(err)
	is.Equal(written, 3)

	insertedUser1 := userTable.Get(is, db, user1.ID)
	insertedUser2 := userTable.Get(is, db, user2.ID)
	insertedUser3 := userTable.Get(is, db, user3.ID)

	is.Equal("", cmp.Diff(user1, insertedUser1))
	is.Equal("", cmp.Diff(user2, insertedUser2))
	is.Equal("", cmp.Diff(user3, insertedUser3))
}

func TestDestination_OperationCreate(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.Connection(t)

	userTable.Recreate(is, db)
	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	src, cleanSrc := testSource(ctx, is)
	defer cleanSrc()

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	rec1 := testutils.ReadAndAssertCreate(ctx, is, sourceIterator{src}, user1)
	rec2 := testutils.ReadAndAssertCreate(ctx, is, sourceIterator{src}, user2)
	rec3 := testutils.ReadAndAssertCreate(ctx, is, sourceIterator{src}, user3)

	// clean table to assert snapshots were written
	userTable.Recreate(is, db)

	written, err := dest.Write(ctx, []opencdc.Record{rec1, rec2, rec3})
	is.NoErr(err)
	is.Equal(written, 3)

	insertedUser1 := userTable.Get(is, db, user1.ID)
	insertedUser2 := userTable.Get(is, db, user2.ID)
	insertedUser3 := userTable.Get(is, db, user3.ID)

	is.Equal("", cmp.Diff(user1, insertedUser1))
	is.Equal("", cmp.Diff(user2, insertedUser2))
	is.Equal("", cmp.Diff(user3, insertedUser3))
}

func TestDestination_OperationUpdate(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.Connection(t)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	src, cleanSrc := testSource(ctx, is)
	defer cleanSrc()

	user1Updated := userTable.Update(is, db, user1.Update())
	user2Updated := userTable.Update(is, db, user2.Update())
	user3Updated := userTable.Update(is, db, user3.Update())

	// discard snapshots, we want the updates only
	testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user2)
	testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user3)

	rec1 := testutils.ReadAndAssertUpdate(ctx, is, sourceIterator{src}, user1, user1Updated)
	rec2 := testutils.ReadAndAssertUpdate(ctx, is, sourceIterator{src}, user2, user2Updated)
	rec3 := testutils.ReadAndAssertUpdate(ctx, is, sourceIterator{src}, user3, user3Updated)

	// clean table to assert snapshots were written
	userTable.Recreate(is, db)

	written, err := dest.Write(ctx, []opencdc.Record{rec1, rec2, rec3})
	is.NoErr(err)
	is.Equal(written, 3)

	insertedUser1 := userTable.Get(is, db, user1.ID)
	insertedUser2 := userTable.Get(is, db, user2.ID)
	insertedUser3 := userTable.Get(is, db, user3.ID)

	is.Equal("", cmp.Diff(user1Updated, insertedUser1))
	is.Equal("", cmp.Diff(user2Updated, insertedUser2))
	is.Equal("", cmp.Diff(user3Updated, insertedUser3))
}

func TestDestination_OperationDelete(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.Connection(t)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	src, cleanSrc := testSource(ctx, is)
	defer cleanSrc()

	userTable.Delete(is, db, user1)
	userTable.Delete(is, db, user2)
	userTable.Delete(is, db, user3)

	// discard snapshots, we want the deletes only
	testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user2)
	testutils.ReadAndAssertSnapshot(ctx, is, sourceIterator{src}, user3)

	rec1 := testutils.ReadAndAssertDelete(ctx, is, sourceIterator{src}, user1)
	rec2 := testutils.ReadAndAssertDelete(ctx, is, sourceIterator{src}, user2)
	rec3 := testutils.ReadAndAssertDelete(ctx, is, sourceIterator{src}, user3)

	// reset autoincrement primary key
	userTable.Recreate(is, db)

	// insert users back so that we can assert that connector deletes the data
	userTable.Insert(is, db, "user1")
	userTable.Insert(is, db, "user2")
	userTable.Insert(is, db, "user3")

	written, err := dest.Write(ctx, []opencdc.Record{rec1, rec2, rec3})
	is.NoErr(err)
	is.Equal(written, 3)

	total := userTable.CountUsers(is, db)
	is.Equal(total, 0)
}
