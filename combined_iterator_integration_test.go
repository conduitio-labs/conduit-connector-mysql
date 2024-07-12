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
	"github.com/matryer/is"
)

func testCombinedIterator(ctx context.Context, is *is.I) (Iterator, func()) {
	iterator, err := newCombinedIterator(ctx, combinedIteratorConfig{
		snapshotConfig: snapshotIteratorConfig{
			tableKeys: testutils.TableKeys,
			db:        testutils.Connection(is),
			database:  "meroxadb",
			tables:    []string{"users"},
		},
		cdcConfig: cdcIteratorConfig{
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

	user1Updated := userTable.Update(is, db, user1.Update())
	user2Updated := userTable.Update(is, db, user2.Update())
	user3Updated := userTable.Update(is, db, user3.Update())

	fmt.Println("starting snapshot asserts")

	readAndAssertSnapshot(ctx, is, iterator, user1)
	readAndAssertSnapshot(ctx, is, iterator, user2)
	readAndAssertSnapshot(ctx, is, iterator, user3)

	fmt.Println("snapshot asserts complete")

	readAndAssertUpdate(ctx, is, iterator, user1, user1Updated)
	readAndAssertUpdate(ctx, is, iterator, user2, user2Updated)
	readAndAssertUpdate(ctx, is, iterator, user3, user3Updated)
}
