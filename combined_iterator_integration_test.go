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
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

func testCombinedIterator(ctx context.Context, is *is.I) (common.Iterator, func()) {
	config, err := mysql.ParseDSN("root:meroxaadmin@tcp(127.0.0.1:3306)/meroxadb?parseTime=true")
	is.NoErr(err)

	serverID := testutils.GetServerID(ctx, is)

	iterator, err := newCombinedIterator(ctx, combinedIteratorConfig{
		snapshotConfig: snapshotIteratorConfig{
			serverID:  serverID,
			tableKeys: testutils.TableKeys,
			db:        testutils.Connection(is),
			database:  "meroxadb",
			tables:    []string{"users"},
		},
		cdcConfig: cdcIteratorConfig{
			mysqlConfig: config,
			tables:      []string{"users"},
			TableKeys:   testutils.TableKeys,
		},
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func TestCombinedIterator(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	iterator, cleanup := testCombinedIterator(ctx, is)
	defer cleanup()

	// ci is slow, we need a bit of time for the setup to initialize canal.Canal.
	// Theoretically it should not matter, as we get the position at the start.
	time.Sleep(time.Second)

	user1Updated := userTable.Update(is, db, user1.Update())
	user2Updated := userTable.Update(is, db, user2.Update())
	user3Updated := userTable.Update(is, db, user3.Update())

	testutils.ReadAndAssertSnapshot(ctx, is, iterator, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, iterator, user2)
	testutils.ReadAndAssertSnapshot(ctx, is, iterator, user3)

	testutils.ReadAndAssertUpdate(ctx, is, iterator, user1, user1Updated)
	testutils.ReadAndAssertUpdate(ctx, is, iterator, user2, user2Updated)
	testutils.ReadAndAssertUpdate(ctx, is, iterator, user3, user3Updated)
}
