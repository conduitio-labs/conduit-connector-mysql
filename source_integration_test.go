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
	"github.com/matryer/is"
)

func testSourceWithFetchSize(
	ctx context.Context,
	is *is.I, fetchSize string,
) (sourceIterator, func()) {
	source := &Source{}
	cfg := config.Config{
		common.SourceConfigDsn:              testutils.DSN,
		common.SourceConfigTables:           "users",
		common.SourceConfigDisableCanalLogs: "true",
	}
	if fetchSize != "" {
		cfg[common.SourceConfigFetchSize] = fetchSize
	}
	err := source.Configure(ctx, cfg)
	is.NoErr(err)

	is.NoErr(source.Open(ctx, nil))

	return sourceIterator{source}, func() { is.NoErr(source.Teardown(ctx)) }
}

func testSource(ctx context.Context, is *is.I) (sourceIterator, func()) {
	return testSourceWithFetchSize(ctx, is, "")
}

type sourceIterator struct{ sdk.Source }

func (s sourceIterator) Next(ctx context.Context) (opencdc.Record, error) {
	//nolint:wrapcheck // wrapped already
	return s.Source.Read(ctx)
}

func TestSource_ConsistentSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	// insert 4 rows, the whole snapshot

	user1 := testutils.InsertUser(is, db, 1)
	user2 := testutils.InsertUser(is, db, 2)
	user3 := testutils.InsertUser(is, db, 3)
	user4 := testutils.InsertUser(is, db, 4)

	// start source connector

	source, teardown := testSource(ctx, is)
	defer teardown()

	// read 2 records -> they shall be snapshots

	testutils.ReadAndAssertSnapshot(ctx, is, source, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, source, user2)

	// insert 2 rows, delete the 4th inserted row

	user5 := testutils.InsertUser(is, db, 5)
	user6 := testutils.InsertUser(is, db, 6)
	testutils.DeleteUser(is, db, user4)

	// read 2 more records -> they shall be snapshots
	// snapshot completed, so previous 2 inserts and delete done while
	// snapshotting should be captured

	testutils.ReadAndAssertSnapshot(ctx, is, source, user3)
	testutils.ReadAndAssertSnapshot(ctx, is, source, user4)

	testutils.ReadAndAssertCreate(ctx, is, source, user5)
	testutils.ReadAndAssertCreate(ctx, is, source, user6)
	testutils.ReadAndAssertDelete(ctx, is, source, user4)
}

func TestSource_NonZeroSnapshotStart(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	// Insert 80 users starting from the 20th so that the starting row's primary key
	// is greater than 0. This ensures a more realistic dataset where
	// the first rows don't start at 0.

	var inserted []testutils.User
	for i := 20; i < 100; i++ {
		user := testutils.InsertUser(is, db, i)
		inserted = append(inserted, user)
	}

	source, teardown := testSourceWithFetchSize(ctx, is, "10")
	defer teardown()

	for _, user := range inserted {
		testutils.ReadAndAssertSnapshot(ctx, is, source, user)
	}
}

func TestSource_EmptyChunkRead(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	var expected []testutils.User
	for i := range 100 {
		userID := i + 1
		if userID > 20 && userID < 40 {
			continue
		} else if userID > 60 && userID < 80 {
			continue
		}

		user := testutils.InsertUser(is, db, userID)
		expected = append(expected, user)
	}

	source, teardown := testSourceWithFetchSize(ctx, is, "10")
	defer teardown()

	for _, user := range expected {
		testutils.ReadAndAssertSnapshot(ctx, is, source, user)
	}
}
