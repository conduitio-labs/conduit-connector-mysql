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
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func testSource(ctx context.Context, is *is.I) (sdk.Source, func()) {
	source := NewSource()
	source.Configure(ctx, common.SourceConfig{
		Config: common.Config{
			URL: testutils.DSN,
		},
		Tables: []string{"users"},
	}.ToMap())

	type logDisabler interface {
		DisableCanalLogs()
	}
	source.(logDisabler).DisableCanalLogs()

	is.NoErr(source.Open(ctx, nil))

	return source, func() { is.NoErr(source.Teardown(ctx)) }
}

func TestSource_ConsistentSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db, closeDB := testutils.Connection(is)
	defer closeDB()

	userTable.Recreate(is, db)

	// insert 4 rows

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")
	user4 := userTable.Insert(is, db, "user4")

	// start source connector

	source, teardown := testSource(ctx, is)
	defer teardown()

	// read 2 records -> they shall be snapshots

	testutils.ReadAndAssertSnapshot(ctx, is, source, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, source, user2)

	// insert 2 rows, delete the 4th inserted row

	user5 := userTable.Insert(is, db, "user5")
	user6 := userTable.Insert(is, db, "user6")
	userTable.Delete(is, db, user4)

	// read 2 more records -> they shall be snapshots

	testutils.ReadAndAssertSnapshot(ctx, is, source, user3)
	testutils.ReadAndAssertSnapshot(ctx, is, source, user4)

	// read 3 records, should be 2 creates and 1 delete

	testutils.ReadAndAssertInsert(ctx, is, source, user5)
	testutils.ReadAndAssertInsert(ctx, is, source, user6)
	testutils.ReadAndAssertDelete(ctx, is, source, user4)
}
