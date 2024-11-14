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
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

func testSnapshotIterator(ctx context.Context, t *testing.T, is *is.I) (common.Iterator, func()) {
	db := testutils.Connection(t)

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

	serverID, err := common.GetServerID(ctx, db)
	is.NoErr(err)

	pos, err := common.ParseSDKPosition(sdkPos)
	is.NoErr(err)

	is.Equal(pos.Kind, common.PositionTypeSnapshot)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tableSortColumns: testutils.TableSortCols,
		db:               db,
		startPosition:    pos.SnapshotPosition,
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

func TestSnapshotIterator_EmptyTable(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.Connection(t)

	testutils.RecreateUsersTable(is, db)

	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	it, cleanup := testSnapshotIterator(ctx, t, is)
	defer cleanup()

	_, err := it.Next(ctx)
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

	_, err := iterator.Next(ctx)
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
			rec, err := it.Next(ctx)
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
		rec, err := it.Next(ctx)
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

	var err error

	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS composite_with_auto_inc;")
	is.NoErr(err)
	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS ulid_pk;")
	is.NoErr(err)
	_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS timestamp_ordered;")
	is.NoErr(err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE composite_with_auto_inc (
			id INT AUTO_INCREMENT PRIMARY KEY,
			tenant_id VARCHAR(50),
			data VARCHAR(100),
			UNIQUE KEY unique_tenant_id (tenant_id, id));`)
	is.NoErr(err)

	compositeWithAutoIncData := []string{"record 1", "record 2", "record 3"}
	_, err = db.ExecContext(ctx, fmt.Sprint(`
		INSERT INTO composite_with_auto_inc (tenant_id, data) VALUES 
			('tenant1', '`, compositeWithAutoIncData[0], `'),
			('tenant2', '`, compositeWithAutoIncData[1], `'),
			('tenant3', '`, compositeWithAutoIncData[2], `');`))
	is.NoErr(err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE ulid_pk (
			id CHAR(26) PRIMARY KEY,
			data VARCHAR(100));`)
	is.NoErr(err)

	ulidPkData := []string{"ULID record 1", "ULID record 2"}
	_, err = db.ExecContext(ctx, fmt.Sprint(`
		INSERT INTO ulid_pk (id, data) VALUES
			('01F8MECHZX3TBDSZ7XRADM79XE', '`, ulidPkData[0], `'),
			('01F8MECHZX3TBDSZ7XRADM79XF', '`, ulidPkData[1], `');`))
	is.NoErr(err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE timestamp_ordered (
			created_at TIMESTAMP(6),
			id VARCHAR(50),
			data VARCHAR(100),
			UNIQUE KEY unique_record (id),
			KEY idx_created_at (created_at));`)
	is.NoErr(err)

	timestampOrderedData := []string{"Timestamp record 1", "Timestamp record 2"}
	now := time.Now().Format("2006-01-02 15:04:05.999999")
	oneSecondAgo := time.Now().Add(-1 * time.Second).Format("2006-01-02 15:04:05.999999")
	_, err = db.ExecContext(ctx, fmt.Sprint(`
		INSERT INTO timestamp_ordered (created_at, id, data) VALUES 
			('`, oneSecondAgo, `', 'rec1', '`, timestampOrderedData[0], `'),
			('`, now, `',          'rec2', '`, timestampOrderedData[1], `');
	`))
	is.NoErr(err)

	type testCase struct {
		tableName    string
		sortingCol   string
		expectedData []string
	}

	for _, testCase := range []testCase{
		{
			tableName:    "composite_with_auto_inc",
			sortingCol:   "id",
			expectedData: compositeWithAutoIncData,
		},
		{
			tableName:    "ulid_pk",
			sortingCol:   "id",
			expectedData: ulidPkData,
		},
		{
			tableName:    "timestamp_ordered",
			sortingCol:   "created_at",
			expectedData: timestampOrderedData,
		},
	} {
		t.Run(fmt.Sprintf("Test table %s", testCase.tableName), func(t *testing.T) {
			db := testutils.Connection(t)

			serverID, err := common.GetServerID(ctx, db)
			is.NoErr(err)

			iterator, err := newSnapshotIterator(snapshotIteratorConfig{
				tableSortColumns: common.TableSortColumns{testCase.tableName: testCase.sortingCol},
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
				rec, err := iterator.Next(ctx)
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

	// fetched end of the worker, now we can delete the last record
	// delete the last record so that fetched worker end isn't found
	_, err = db.ExecContext(ctx, "DELETE FROM users WHERE id = 100")
	is.NoErr(err)

	iterator.start(ctx)
	defer func() {
		is.NoErr(db.Close())
		is.NoErr(iterator.Teardown(ctx))
	}()

	for i := 1; i <= 99; i++ {
		rec, err := iterator.Next(ctx)
		if errors.Is(err, ErrSnapshotIteratorDone) {
			break
		} else {
			is.NoErr(err)
		}
		is.NoErr(iterator.Ack(ctx, rec.Position))

		testutils.AssertUserSnapshot(is, users[i-1], rec)
	}
}
