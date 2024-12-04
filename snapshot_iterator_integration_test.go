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
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

func testSnapshotIterator(ctx context.Context, t *testing.T, is *is.I) (common.Iterator, func()) {
	db := testutils.Connection(t).Conn()

	serverID, err := common.GetServerID(ctx, db)
	is.NoErr(err)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tableSortColumns: testutils.TableSortCols,
		db:               db,
		database:         "meroxadb",
		tables:           []string{"users"},
		serverID:         serverID,
	})
	is.NoErr(err)

	is.NoErr(iterator.setupWorkers(ctx))
	iterator.start(ctx)

	return iterator, func() {
		is.NoErr(db.Close())
		is.NoErr(iterator.Teardown(ctx))
	}
}

func testSnapshotIteratorAtPosition(
	ctx context.Context, t *testing.T, is *is.I,
	sdkPos opencdc.Position,
) (common.Iterator, func()) {
	db := testutils.Connection(t)

	serverID, err := common.GetServerID(ctx, db.Conn())
	is.NoErr(err)

	pos, err := common.ParseSDKPosition(sdkPos)
	is.NoErr(err)

	is.Equal(pos.Kind, common.PositionTypeSnapshot)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tableSortColumns: testutils.TableSortCols,
		db:               db.Conn(),
		startPosition:    pos.SnapshotPosition,
		database:         "meroxadb",
		tables:           []string{"users"},
		serverID:         serverID,
	})
	is.NoErr(err)

	is.NoErr(iterator.setupWorkers(ctx))
	iterator.start(ctx)

	return iterator, func() {
		is.NoErr(db.Conn().Close())
		is.NoErr(iterator.Teardown(ctx))
	}
}

func TestSnapshotIterator_EmptyTable(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	it, cleanup := testSnapshotIterator(ctx, t, is)
	defer cleanup()

	_, err := it.Read(ctx)
	if !errors.Is(err, ErrSnapshotIteratorDone) {
		is.NoErr(err)
	}
}

func TestSnapshotIterator_WithData(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	var users []testutils.User
	for i := 1; i <= 100; i++ {
		user := testutils.InsertUser(is, db, i)
		users = append(users, user)
	}

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	iterator, cleanup := testSnapshotIterator(ctx, t, is)
	defer cleanup()

	for i := 1; i <= 100; i++ {
		testutils.ReadAndAssertSnapshot(ctx, is, iterator, users[i-1])
	}

	_, err := iterator.Read(ctx)
	is.True(errors.Is(err, ErrSnapshotIteratorDone))
}

func TestSnapshotIterator_RestartOnPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)
	var users []testutils.User
	for i := 1; i <= 100; i++ {
		user := testutils.InsertUser(is, db, i)
		users = append(users, user)
	}

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	var recs []opencdc.Record
	var breakPosition opencdc.Position
	{
		it, cleanup := testSnapshotIterator(ctx, t, is)

		for i := 1; i <= 10; i++ {
			rec, err := it.Read(ctx)
			if errors.Is(err, ErrSnapshotIteratorDone) {
				err = it.Ack(ctx, rec.Position)
				is.NoErr(err)
				break
			}
			is.NoErr(err)

			recs = append(recs, rec)

			err = it.Ack(ctx, rec.Position)
			is.NoErr(err)
			breakPosition = rec.Position
		}

		// not deferring the call so that logs are easier to understand
		cleanup()
	}

	// read the remaining 90 records

	it, cleanup := testSnapshotIteratorAtPosition(ctx, t, is, breakPosition)
	defer cleanup()

	for {
		rec, err := it.Read(ctx)
		if errors.Is(err, ErrSnapshotIteratorDone) {
			break
		}
		is.NoErr(err)

		recs = append(recs, rec)

		err = it.Ack(ctx, rec.Position)
		is.NoErr(err)
	}

	is.Equal(len(recs), 100)
	for i, rec := range recs {
		testutils.AssertUserSnapshot(is, users[i], rec)
	}
}

func TestSnapshotIterator_CustomTableKeys(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	db := testutils.Connection(t)

	type CompositeWithAutoInc struct {
		ID       int    `gorm:"primaryKey;autoIncrement"`
		TenantID string `gorm:"size:50"`
		Data     string `gorm:"size:100"`
	}

	type UlidPk struct {
		ID   string `gorm:"primaryKey;size:26"`
		Data string `gorm:"size:100"`
	}

	type TimestampOrdered struct {
		CreatedAt time.Time `gorm:"index:idx_created_at"`
		ID        string    `gorm:"size:50;uniqueIndex:unique_record"`
		Data      string    `gorm:"size:100"`
	}

	is.NoErr(db.Migrator().DropTable(&CompositeWithAutoInc{}))
	is.NoErr(db.Migrator().DropTable(&UlidPk{}))
	is.NoErr(db.Migrator().DropTable(&TimestampOrdered{}))

	is.NoErr(db.AutoMigrate(&CompositeWithAutoInc{}, &UlidPk{}, &TimestampOrdered{}))

	compositeWithAutoIncData := []CompositeWithAutoInc{
		{TenantID: "tenant1", Data: "record 1"},
		{TenantID: "tenant2", Data: "record 2"},
		{TenantID: "tenant3", Data: "record 3"},
	}
	is.NoErr(db.Create(&compositeWithAutoIncData).Error)

	ulidPkData := []UlidPk{
		{ID: "01F8MECHZX3TBDSZ7XRADM79XE", Data: "ULID record 1"},
		{ID: "01F8MECHZX3TBDSZ7XRADM79XF", Data: "ULID record 2"},
	}
	is.NoErr(db.Create(&ulidPkData).Error)

	now := time.Now()
	oneSecondAgo := now.Add(-1 * time.Second)
	timestampOrderedData := []TimestampOrdered{
		{CreatedAt: oneSecondAgo, ID: "rec1", Data: "Timestamp record 1"},
		{CreatedAt: now, ID: "rec2", Data: "Timestamp record 2"},
	}
	is.NoErr(db.Create(&timestampOrderedData).Error)

	type testCase struct {
		tableName    string
		sortingCol   string
		expectedData []string
	}

	for _, testCase := range []testCase{
		{
			tableName:    "composite_with_auto_inc",
			sortingCol:   "id",
			expectedData: []string{"record 1", "record 2", "record 3"},
		},
		{
			tableName:    "ulid_pk",
			sortingCol:   "id",
			expectedData: []string{"ULID record 1", "ULID record 2"},
		},
		{
			tableName:    "timestamp_ordered",
			sortingCol:   "created_at",
			expectedData: []string{"Timestamp record 1", "Timestamp record 2"},
		},
	} {
		t.Run(fmt.Sprintf("Test table %s", testCase.tableName), func(t *testing.T) {
			db := testutils.Connection(t).Conn()

			serverID, err := common.GetServerID(ctx, db)
			is.NoErr(err)

			iterator, err := newSnapshotIterator(snapshotIteratorConfig{
				tableSortColumns: map[string]string{testCase.tableName: testCase.sortingCol},
				db:               db,
				database:         "meroxadb",
				tables:           []string{testCase.tableName},
				serverID:         serverID,
			})
			is.NoErr(err)

			is.NoErr(iterator.setupWorkers(ctx))
			iterator.start(ctx)

			var recs []opencdc.Record
			for {
				rec, err := iterator.Read(ctx)
				if errors.Is(err, ErrSnapshotIteratorDone) {
					break
				}
				is.NoErr(err)

				err = iterator.Ack(ctx, rec.Position)
				is.NoErr(err)

				recs = append(recs, rec)
			}

			is.Equal(len(recs), len(testCase.expectedData))

			for i, expectedData := range testCase.expectedData {
				actual := recs[i]
				is.Equal(actual.Operation, opencdc.OperationSnapshot)
				is.Equal(actual.Payload.After.(opencdc.StructuredData)["data"].(string), expectedData)
			}

			is.NoErr(iterator.Teardown(ctx))
		})
	}
}

func TestSnapshotIterator_DeleteEndWhileSnapshotting(t *testing.T) {
	// Asserts that the snapshot still works even if data is deleted after getting
	// the snapshot limits.

	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)
	conn := db.Conn()
	testutils.RecreateUsersTable(is, db)

	var users []testutils.User
	for i := 1; i <= 100; i++ {
		user := testutils.InsertUser(is, db, i)
		users = append(users, user)
	}

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	serverID, err := common.GetServerID(ctx, conn)
	is.NoErr(err)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tableSortColumns: testutils.TableSortCols,
		db:               conn,
		database:         "meroxadb",
		tables:           []string{"users"},
		serverID:         serverID,
	})
	is.NoErr(err)

	is.NoErr(iterator.setupWorkers(ctx))

	randomUserIndex := rand.IntN(len(users))
	is.NoErr(db.Delete(users[randomUserIndex]).Error)
	users = append(users[:randomUserIndex], users[randomUserIndex+1:]...)

	iterator.start(ctx)
	defer func() {
		is.NoErr(conn.Close())
		is.NoErr(iterator.Teardown(ctx))
	}()

	for _, user := range users {
		rec, err := iterator.Read(ctx)
		is.NoErr(err)
		is.NoErr(iterator.Ack(ctx, rec.Position))

		testutils.AssertUserSnapshot(is, user, rec)
	}

	_, err = iterator.Read(ctx)
	is.True(errors.Is(err, ErrSnapshotIteratorDone))
}

func TestSnapshotIterator_StringSorting(t *testing.T) {
	// This test ensures that we sort snapshot rows when using a string column as a
	// custom sorting column.

	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	type Table struct {
		ID  int    `gorm:"primaryKey;autoIncrement"`
		Str string `gorm:"size:50"`
	}

	is.NoErr(db.Migrator().DropTable(&Table{}))

	is.NoErr(db.AutoMigrate(&Table{}))

	data := []Table{
		{Str: "Zebra"},
		{Str: "apple"},
		{Str: "BANANA"},
		{Str: "āpple"},
		{Str: "_apple"},
		{Str: "123apple"},
		{Str: "Apple"},
	}

	sorted := []Table{
		{Str: "_apple"},
		{Str: "123apple"},
		{Str: "apple"},
		{Str: "āpple"},
		{Str: "Apple"},
		{Str: "BANANA"},
		{Str: "Zebra"},
	}

	is.NoErr(db.Create(&data).Error)

	serverID, err := common.GetServerID(ctx, db.Conn())
	is.NoErr(err)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tableSortColumns: map[string]string{"string_sorting": "str"},
		db:               db.Conn(),
		database:         "meroxadb",
		tables:           []string{"string_sorting"},
		serverID:         serverID,
	})
	is.NoErr(err)

	is.NoErr(iterator.setupWorkers(ctx))
	iterator.start(ctx)

	var recs []opencdc.Record
	for {
		rec, err := iterator.Read(ctx)
		if errors.Is(err, ErrSnapshotIteratorDone) {
			break
		}
		is.NoErr(err)

		err = iterator.Ack(ctx, rec.Position)
		is.NoErr(err)

		recs = append(recs, rec)
	}

	is.Equal(len(recs), len(sorted))

	for i, expectedData := range sorted {
		actual := recs[i]
		is.Equal(actual.Operation, opencdc.OperationSnapshot)
		is.Equal(actual.Payload.After.(opencdc.StructuredData)["str"].(string), expectedData.Str)
	}

	is.NoErr(iterator.Teardown(ctx))
}
