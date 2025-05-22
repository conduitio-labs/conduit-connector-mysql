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
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

func testCombinedIterator(ctx context.Context, t *testing.T, is *is.I) (common.Iterator, func()) {
	db := testutils.NewDB(t).SqlxDB

	config, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)

	iterator, err := newCombinedIterator(ctx, combinedIteratorConfig{
		db: db,
		tableKeys: filteredTableKeys{
			Snapshot: testutils.TablePrimaryKeys,
			Cdc: CdcTableKeys{
				TableRegexes: []string{"users"},
				TableKeys:    testutils.TablePrimaryKeys,
			},
		},
		database:            testutils.Database,
		serverID:            testutils.ServerID,
		mysqlConfig:         config,
		disableCanalLogging: true,
		snapshotEnabled:     true,
	})
	is.NoErr(err)

	return iterator, func() { is.NoErr(iterator.Teardown(ctx)) }
}

func TestCombinedIterator_SnapshotAndCDC(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)

	user1 := testutils.InsertUser(is, db, 1)
	user2 := testutils.InsertUser(is, db, 2)
	user3 := testutils.InsertUser(is, db, 3)

	iterator, cleanup := testCombinedIterator(ctx, t, is)
	defer cleanup()

	user1Updated := testutils.UpdateUser(is, db, user1.Update())
	user2Updated := testutils.UpdateUser(is, db, user2.Update())
	user3Updated := testutils.UpdateUser(is, db, user3.Update())

	testutils.ReadAndAssertSnapshot(ctx, is, iterator, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, iterator, user2)
	testutils.ReadAndAssertSnapshot(ctx, is, iterator, user3)

	testutils.ReadAndAssertUpdate(ctx, is, iterator, user1, user1Updated)
	testutils.ReadAndAssertUpdate(ctx, is, iterator, user2, user2Updated)
	testutils.ReadAndAssertUpdate(ctx, is, iterator, user3, user3Updated)
}
