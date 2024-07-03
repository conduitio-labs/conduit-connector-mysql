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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func createTestConnection(is *is.I) *sqlx.DB {
	db, err := sqlx.Open("mysql", "root:meroxaadmin@tcp(127.0.0.1:3306)/meroxadb")
	is.NoErr(err)

	return db
}

func testContext(t *testing.T) context.Context {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	return logger.WithContext(context.Background())
}

var (
	tables           = []string{"users", "orders"}
	totalRowsPerTabl = 50
)

type TestTables struct{}

var testTables TestTables

func (TestTables) drop(is *is.I, db *sqlx.DB) {
	dropOrdersTableQuery := `DROP TABLE IF EXISTS orders`
	_, err := db.Exec(dropOrdersTableQuery)
	is.NoErr(err)

	dropUsersTableQuery := `DROP TABLE IF EXISTS users`
	_, err = db.Exec(dropUsersTableQuery)
	is.NoErr(err)
}

func (TestTables) create(is *is.I, db *sqlx.DB) {
	createUsersTableQuery := `
	CREATE TABLE users (
		id INT AUTO_INCREMENT PRIMARY KEY,
		username VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	_, err := db.Exec(createUsersTableQuery)
	is.NoErr(err)

	// Create orders table
	createOrdersTableQuery := `
	CREATE TABLE orders (
		id INT AUTO_INCREMENT PRIMARY KEY,
		user_id INT,
		product VARCHAR(255) NOT NULL,
		amount DECIMAL(10, 2) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	_, err = db.Exec(createOrdersTableQuery)
	is.NoErr(err)
}

func (TestTables) insertData(is *is.I, db *sqlx.DB) {
	for i := 1; i <= totalRowsPerTabl; i++ {
		insertUsersRowQuery := fmt.Sprintf(`
		INSERT INTO users (username, email) 
		VALUES ('user%d', 'user%d@example.com')`, i, i)
		_, err := db.Exec(insertUsersRowQuery)
		is.NoErr(err)
	}

	for i := 1; i <= totalRowsPerTabl; i++ {
		insertOrdersRowQuery := fmt.Sprintf(`
		INSERT INTO orders (user_id, product, amount) 
		VALUES (%d, 'product%d', %.2f)`, i, i, float64(i)*10.0)
		_, err := db.Exec(insertOrdersRowQuery)
		is.NoErr(err)
	}
}

func TestSnapshotIterator_EmptyTable(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)

	db := createTestConnection(is)

	testTables.drop(is, db)
	testTables.create(is, db)

	it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		db:       db,
		database: "meroxadb",
		tables:   tables,
	})
	is.NoErr(err)
	defer func() { is.NoErr(it.Teardown(ctx)) }()

	_, err = it.Next(ctx)
	if errors.Is(err, ErrIteratorDone) {
		return
	}
	is.NoErr(err)
}

func TestSnapshotIterator_MultipleTables(t *testing.T) {
	ctx := testContext(t)

	is := is.New(t)

	db := createTestConnection(is)

	testTables.drop(is, db)
	testTables.create(is, db)
	testTables.insertData(is, db)

	it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		db:       db,
		database: "meroxadb",
		tables:   tables,
	})
	is.NoErr(err)
	defer func() { is.NoErr(it.Teardown(ctx)) }()

	var recs []sdk.Record

	for {
		rec, err := it.Next(ctx)
		if errors.Is(err, ErrIteratorDone) {
			break
		}
		is.NoErr(err)

		recs = append(recs, rec)

		err = it.Ack(ctx, rec.Position)
		is.NoErr(err)
	}

	is.Equal(len(recs), 100) // received a different amount of records
	for _, rec := range recs {
		is.Equal(rec.Operation, sdk.OperationSnapshot)
	}
}

func TestSnapshotIterator_SmallFetchSize(t *testing.T) {
	ctx := testContext(t)
	is := is.New(t)

	db := createTestConnection(is)

	testTables.drop(is, db)
	testTables.create(is, db)
	testTables.insertData(is, db)

	it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		db:       db,
		database: "meroxadb",
		tables:   tables,
	})
	is.NoErr(err)
	defer func() { is.NoErr(it.Teardown(ctx)) }()

	var recs []sdk.Record

	for {
		rec, err := it.Next(ctx)
		if errors.Is(err, ErrIteratorDone) {
			break
		}
		is.NoErr(err)

		recs = append(recs, rec)

		err = it.Ack(ctx, rec.Position)
		is.NoErr(err)
	}

	is.Equal(len(recs), 100) // received a different amount of records
	for _, rec := range recs {
		is.Equal(rec.Operation, sdk.OperationSnapshot)
	}
}

func TestSnapshotIterator_RestartOnPosition(t *testing.T) {
	ctx := testContext(t)

	is := is.New(t)

	db := createTestConnection(is)

	testTables.drop(is, db)
	testTables.create(is, db)
	testTables.insertData(is, db)

	// Read the first 10 records

	var breakPosition position
	{
		it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
			db:       db,
			database: "meroxadb",
			tables:   tables,
		})
		is.NoErr(err)
		defer func() { is.NoErr(it.Teardown(ctx)) }()

		var recs []sdk.Record
		for i := 0; i < 10; i++ {
			rec, err := it.Next(ctx)
			if errors.Is(err, ErrIteratorDone) {
				break
			}
			is.NoErr(err)

			recs = append(recs, rec)

			err = it.Ack(ctx, rec.Position)
			is.NoErr(err)
		}

		breakPosition, err = parseSDKPosition(recs[len(recs)-1].Position)
		is.NoErr(err)
	}

	// read the remaining 90 records

	it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		db:            db,
		startPosition: breakPosition,
		database:      "meroxadb",
		tables:        tables,
	})
	is.NoErr(err)

	var recs []sdk.Record
	for {
		rec, err := it.Next(ctx)
		if errors.Is(err, ErrIteratorDone) {
			break
		}
		is.NoErr(err)

		recs = append(recs, rec)

		err = it.Ack(ctx, rec.Position)
		is.NoErr(err)
	}

	is.Equal(len(recs), 90)
	for _, rec := range recs {
		is.Equal(rec.Operation, sdk.OperationSnapshot)
	}
}
