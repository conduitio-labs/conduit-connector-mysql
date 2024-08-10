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

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

type sourceIterator struct {
	sdk.Source
}

func (s sourceIterator) Next(ctx context.Context) (sdk.Record, error) {
	//nolint:wrapcheck // wrapped already
	return s.Source.Read(ctx)
}

func TestSourceWorks(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.Connection(is)

	userTable.Recreate(is, db)

	user1 := userTable.Insert(is, db, "user1")
	user2 := userTable.Insert(is, db, "user2")
	user3 := userTable.Insert(is, db, "user3")

	source := sourceIterator{NewSource()}
	is.NoErr(source.Configure(ctx, map[string]string{
		"url":    "root:meroxaadmin@tcp(127.0.0.1:3306)/meroxadb?parseTime=true",
		"tables": "users",
	}))

	is.NoErr(source.Open(ctx, nil))

	// ci is slow, we need a bit of time for the setup to initialize canal.Canal.
	// Theoretically it should not matter, as we get the position at the start.
	time.Sleep(time.Second)

	user1Updated := userTable.Update(is, db, user1.Update())
	user2Updated := userTable.Update(is, db, user2.Update())
	user3Updated := userTable.Update(is, db, user3.Update())

	testutils.ReadAndAssertSnapshot(ctx, is, source, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, source, user2)
	testutils.ReadAndAssertSnapshot(ctx, is, source, user3)

	testutils.ReadAndAssertUpdate(ctx, is, source, user1, user1Updated)
	testutils.ReadAndAssertUpdate(ctx, is, source, user2, user2Updated)
	testutils.ReadAndAssertUpdate(ctx, is, source, user3, user3Updated)
}
