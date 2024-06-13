package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func createTestConnection(is *is.I) *sql.DB {
	db, err := connect(Config{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "meroxaadmin",
		Database: "meroxadb",
	})
	is.NoErr(err)

	return db
}

var tables = []string{"users", "orders"}
var totalRowsPerTabl = 50

type TestTables struct{}

var testTables TestTables

func (TestTables) drop(is *is.I, db *sql.DB) {
	dropOrdersTableQuery := `DROP TABLE IF EXISTS orders`
	_, err := db.Exec(dropOrdersTableQuery)
	is.NoErr(err)

	dropUsersTableQuery := `DROP TABLE IF EXISTS users`
	_, err = db.Exec(dropUsersTableQuery)
	is.NoErr(err)
}

func (TestTables) create(is *is.I, db *sql.DB) {
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

func (TestTables) insertData(is *is.I, db *sql.DB) {
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
	ctx := context.Background()
	is := is.New(t)

	db := createTestConnection(is)

	testTables.drop(is, db)
	testTables.create(is, db)

	it, err := newSnapshotIterator(ctx, db, tables)
	is.NoErr(err)

	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = it.Next(ctx)
	if errors.Is(err, ErrIteratorDone) {
		return
	}
	is.NoErr(err)
}

func TestSnapshotIterator_MultipleTables(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	db := createTestConnection(is)

	testTables.drop(is, db)
	testTables.create(is, db)
	testTables.insertData(is, db)

	it, err := newSnapshotIterator(ctx, db, tables)
	is.NoErr(err)

	var recs []sdk.Record
	for {
		ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
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
