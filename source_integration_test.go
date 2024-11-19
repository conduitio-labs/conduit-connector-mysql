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
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func testSource(ctx context.Context, is *is.I, cfg config.Config) (sdk.Source, func()) {
	source := &Source{}
	cfg[common.SourceConfigDsn] = testutils.DSN
	cfg[common.SourceConfigDisableCanalLogs] = "true"

	err := source.Configure(ctx, cfg)
	is.NoErr(err)

	is.NoErr(source.Open(ctx, nil))

	return source, func() { is.NoErr(source.Teardown(ctx)) }
}

func testSourceFromUsers(ctx context.Context, is *is.I) (sdk.Source, func()) {
	return testSource(ctx, is, config.Config{
		common.SourceConfigTables: "users",
	})
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

	source, teardown := testSource(ctx, is, config.Config{
		common.SourceConfigTables: "users",
	})
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

	source, teardown := testSource(ctx, is, config.Config{
		common.SourceConfigTables:    "users",
		common.SourceConfigFetchSize: "10",
	})
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

	source, teardown := testSource(ctx, is, config.Config{
		common.SourceConfigTables:    "users",
		common.SourceConfigFetchSize: "10",
	})
	defer teardown()

	for _, user := range expected {
		testutils.ReadAndAssertSnapshot(ctx, is, source, user)
	}
}

func TestUnsafeSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS table_with_pk;")
	is.NoErr(err)
	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS table_without_pk;")
	is.NoErr(err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE table_with_pk (
			id INT PRIMARY KEY,
			data VARCHAR(100));`)
	is.NoErr(err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE table_without_pk (
			id INT,
			data VARCHAR(100));`)
	is.NoErr(err)

	tableWithPkData := []string{"record 1", "record 2", "record 3"}
	_, err = db.ExecContext(ctx, fmt.Sprint(`
		INSERT INTO table_with_pk (id, data) VALUES
			(1, '`, tableWithPkData[0], `'),
			(2, '`, tableWithPkData[1], `'),
			(3, '`, tableWithPkData[2], `');`))
	is.NoErr(err)

	tableWithoutPkData := []string{"record A", "record B"}
	_, err = db.ExecContext(ctx, fmt.Sprint(`
		INSERT INTO table_without_pk (id, data) VALUES
			(1, '`, tableWithoutPkData[0], `'),
			(2, '`, tableWithoutPkData[1], `');`))
	is.NoErr(err)

	type testCase struct {
		tableName    string
		expectedData []string
	}

	for _, testCase := range []testCase{
		{
			tableName:    "table_with_pk",
			expectedData: tableWithPkData,
		},
		{
			tableName:    "table_without_pk",
			expectedData: tableWithoutPkData,
		},
	} {
		t.Run(fmt.Sprintf("Test table %s", testCase.tableName), func(t *testing.T) {
			source, teardown := testSource(ctx, is, config.Config{
				common.SourceConfigTables: testCase.tableName,
			})
			defer teardown()

			var recs []opencdc.Record
			for i := 0; i < len(testCase.expectedData); i++ {
				rec, err := source.Read(ctx)
				is.NoErr(err)
				is.NoErr(source.Ack(ctx, rec.Position))

				recs = append(recs, rec)
			}

			for i, expectedData := range testCase.expectedData {
				actual := recs[i]
				is.Equal(actual.Operation, opencdc.OperationSnapshot)
				is.Equal(actual.Payload.After.(opencdc.StructuredData)["data"].(string), expectedData)
			}
		})
	}
}
