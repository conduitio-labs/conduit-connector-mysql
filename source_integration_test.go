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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/matryer/is"
)

func testSource(ctx context.Context, is *is.I, cfg map[string]string) (sdk.Source, func()) {
	source := &Source{}

	cfg["dsn"] = testutils.DSN
	cfg["cdc.disableLogs"] = "true"

	err := sdk.Util.ParseConfig(ctx,
		cfg, source.Config(),
		Connector.NewSpecification().SourceParams,
	)
	is.NoErr(err)

	is.NoErr(source.Open(ctx, nil))

	return source, func() { is.NoErr(source.Teardown(ctx)) }
}

func testSourceFromUsers(ctx context.Context, is *is.I) (sdk.Source, func()) {
	return testSource(ctx, is, map[string]string{
		"tables": "users",
	})
}

func TestSource_ConsistentSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)

	// insert 4 rows, the whole snapshot

	user1 := testutils.InsertUser(is, db, 1)
	user2 := testutils.InsertUser(is, db, 2)
	user3 := testutils.InsertUser(is, db, 3)
	user4 := testutils.InsertUser(is, db, 4)

	// start source connector

	source, teardown := testSourceFromUsers(ctx, is)
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

	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)

	// Insert 80 users starting from the 20th so that the starting row's primary key
	// is greater than 0. This ensures a more realistic dataset where
	// the first rows don't start at 0.

	var inserted []testutils.User
	for i := 20; i < 100; i++ {
		user := testutils.InsertUser(is, db, i)
		inserted = append(inserted, user)
	}

	source, teardown := testSource(ctx, is, map[string]string{
		"tables":             "users",
		"snapshot.fetchSize": "10",
	})
	defer teardown()

	for _, user := range inserted {
		testutils.ReadAndAssertSnapshot(ctx, is, source, user)
	}
}

func TestSource_EmptyChunkRead(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)

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

	source, teardown := testSource(ctx, is, map[string]string{
		"tables":             "users",
		"snapshot.fetchSize": "10",
	})
	defer teardown()

	for _, user := range expected {
		testutils.ReadAndAssertSnapshot(ctx, is, source, user)
	}
}

func TestSource_UnsafeSnapshot(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)

	type TableWithoutPK struct {
		// No id field, forcing gorm to not create a primary key

		Data string `gorm:"size:100"`
	}

	testutils.CreateTables(is, db, &TableWithoutPK{})

	db.Create([]TableWithoutPK{
		{Data: "record A"},
		{Data: "record B"},
	})
	is.NoErr(db.Error)

	expectedData := []string{"record A", "record B"}

	ctx := testutils.TestContext(t)
	source, teardown := testSource(ctx, is, map[string]string{
		"tables":          "table_without_pk",
		"snapshot.unsafe": "true",
	})
	defer teardown()

	var recs []opencdc.Record
	for range expectedData {
		rec, err := source.ReadN(ctx, 1)
		is.NoErr(err)
		is.True(len(rec) == 1)
		is.NoErr(source.Ack(ctx, rec[0].Position))

		recs = append(recs, rec[0])
	}

	for i, expectedData := range expectedData {
		actual := recs[i]
		is.Equal(actual.Operation, opencdc.OperationSnapshot)
		is.Equal(actual.Payload.After.(opencdc.StructuredData)["data"].(string), expectedData)
	}
}

func TestSource_CompositeKey(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)
	db := testutils.NewDB(t)

	type CompositeKeyTable struct {
		UserID    int    `gorm:"primaryKey;autoIncrement:false"`
		TenantID  string `gorm:"primaryKey;size:50"`
		Email     string `gorm:"size:100;uniqueIndex"`
		FirstName string `gorm:"size:50"`
		LastName  string `gorm:"size:50"`
	}

	tableName := testutils.TableName(is, db, &CompositeKeyTable{})

	testutils.CreateTables(is, db, &CompositeKeyTable{})

	testData := []CompositeKeyTable{
		{UserID: 1, TenantID: "tenant1", Email: "user1@example.com", FirstName: "John", LastName: "Doe"},
		{UserID: 2, TenantID: "tenant1", Email: "user2@example.com", FirstName: "Jane", LastName: "Smith"},
		{UserID: 1, TenantID: "tenant2", Email: "user3@example.com", FirstName: "Alice", LastName: "Johnson"},
		{UserID: 2, TenantID: "tenant2", Email: "user4@example.com", FirstName: "Bob", LastName: "Brown"},
	}
	is.NoErr(db.Create(&testData).Error)

	source, teardown := testSource(ctx, is, map[string]string{
		"tables": tableName,
	})
	defer teardown()

	var snapshotRecords []opencdc.Record
	for range testData {
		recs, err := source.ReadN(ctx, 1)
		is.NoErr(err)
		is.True(len(recs) == 1)
		rec := recs[0]
		is.NoErr(source.Ack(ctx, rec.Position))

		is.Equal(rec.Operation, opencdc.OperationSnapshot)

		collection, err := rec.Metadata.GetCollection()
		is.NoErr(err)
		is.Equal(collection, tableName)

		payloadSchemaSubject, err := rec.Metadata.GetPayloadSchemaSubject()
		is.NoErr(err)
		payloadSchemaVersion, err := rec.Metadata.GetPayloadSchemaVersion()
		is.NoErr(err)

		keySchemaSubject, err := rec.Metadata.GetKeySchemaSubject()
		is.NoErr(err)
		keySchemaVersion, err := rec.Metadata.GetKeySchemaVersion()
		is.NoErr(err)

		payloadSchema, err := schema.Get(ctx, payloadSchemaSubject, payloadSchemaVersion)
		is.NoErr(err)

		keySchema, err := schema.Get(ctx, keySchemaSubject, keySchemaVersion)
		is.NoErr(err)

		var parsedPayloadSchema testutils.AvroSchema
		is.NoErr(json.Unmarshal(payloadSchema.Bytes, &parsedPayloadSchema))

		var parsedKeySchema testutils.AvroSchema
		is.NoErr(json.Unmarshal(keySchema.Bytes, &parsedKeySchema))

		is.Equal(parsedPayloadSchema.Type, "record")
		is.Equal(len(parsedPayloadSchema.Fields), 5)

		is.Equal(parsedKeySchema.Type, "record")
		is.Equal(len(parsedKeySchema.Fields), 2)

		var keyFieldNames []string
		for _, field := range parsedKeySchema.Fields {
			keyFieldNames = append(keyFieldNames, field.Name)
		}
		is.True(slices.Contains(keyFieldNames, "user_id"))
		is.True(slices.Contains(keyFieldNames, "tenant_id"))

		snapshotRecords = append(snapshotRecords, rec)
	}

	is.Equal(len(snapshotRecords), len(testData))

	for _, rec := range snapshotRecords {
		key, ok := rec.Key.(opencdc.StructuredData)
		is.True(ok)

		_, hasUserID := key["user_id"]
		_, hasTenantID := key["tenant_id"]
		is.True(hasUserID)
		is.True(hasTenantID)
	}

	updatedRecord := testData[0]
	updatedRecord.FirstName = "Johnny"
	updatedRecord.LastName = "Doeson"
	is.NoErr(db.Save(&updatedRecord).Error)

	newRecord := CompositeKeyTable{
		UserID: 3, TenantID: "tenant1", Email: "user5@example.com",
		FirstName: "Sarah", LastName: "Wilson",
	}
	is.NoErr(db.Create(&newRecord).Error)

	is.NoErr(db.Delete(&testData[3]).Error)

	recs, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1)
	updateRec := recs[0]
	is.NoErr(source.Ack(ctx, updateRec.Position))
	is.Equal(updateRec.Operation, opencdc.OperationUpdate)

	is.True(updateRec.Payload.Before != nil)
	is.True(updateRec.Payload.After != nil)

	createRecs, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(createRecs) == 1)
	createRec := createRecs[0]
	is.NoErr(source.Ack(ctx, createRec.Position))
	is.Equal(createRec.Operation, opencdc.OperationCreate)

	deleteRecs, err := source.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(deleteRecs) == 1)
	deleteRec := deleteRecs[0]
	is.NoErr(source.Ack(ctx, deleteRec.Position))
	is.Equal(deleteRec.Operation, opencdc.OperationDelete)

	is.True(deleteRec.Payload.Before != nil)
	is.True(deleteRec.Payload.After == nil)

	for _, rec := range []opencdc.Record{updateRec, createRec, deleteRec} {
		key, ok := rec.Key.(opencdc.StructuredData)
		is.True(ok)

		_, hasUserID := key["user_id"]
		_, hasTenantID := key["tenant_id"]
		is.True(hasUserID)
		is.True(hasTenantID)

		keySchemaSubject, err := rec.Metadata.GetKeySchemaSubject()
		is.NoErr(err)
		keySchemaVersion, err := rec.Metadata.GetKeySchemaVersion()
		is.NoErr(err)

		keySchema, err := schema.Get(ctx, keySchemaSubject, keySchemaVersion)
		is.NoErr(err)

		var parsedKeySchema testutils.AvroSchema
		is.NoErr(json.Unmarshal(keySchema.Bytes, &parsedKeySchema))

		is.Equal(parsedKeySchema.Type, "record")
		is.Equal(len(parsedKeySchema.Fields), 2) // Both parts of the composite key
	}
}

func TestNoSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)

	user1 := testutils.InsertUser(is, db, 1)
	user2 := testutils.InsertUser(is, db, 2)

	source, teardown := testSource(ctx, is, map[string]string{
		"tables":           "users",
		"snapshot.enabled": "true",
	})
	defer teardown()

	user3 := testutils.InsertUser(is, db, 3)

	user1Before := user1
	user1Updated := user1.Update()
	testutils.UpdateUser(is, db, user1Updated)

	testutils.DeleteUser(is, db, user2)

	// We should only get CDC events (no snapshot records)
	testutils.ReadAndAssertCreate(ctx, is, source, user3)
	testutils.ReadAndAssertUpdate(ctx, is, source, user1Before, user1Updated)
	testutils.ReadAndAssertDelete(ctx, is, source, user2)

	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	_, err := source.ReadN(ctx, 1)
	fmt.Println(err)
	is.True(errors.Is(err, context.DeadlineExceeded))
}
