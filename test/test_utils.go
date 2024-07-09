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

package testutils

import (
	"context"
	"fmt"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestConnection(is *is.I) *sqlx.DB {
	db, err := sqlx.Open("mysql", "root:meroxaadmin@tcp(127.0.0.1:3306)/meroxadb?parseTime=true")
	is.NoErr(err)

	return db
}

func TestContext(t *testing.T) context.Context {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	return logger.WithContext(context.Background())
}

var totalRowsPerTabl = 50

type TestTables struct{}

func (TestTables) Tables() []string {
	return []string{"users", "orders"}
}

func (TestTables) TableKeys() map[string]string {
	return map[string]string{
		"users":  "id",
		"orders": "id",
	}
}

func (TestTables) Drop(is *is.I, db *sqlx.DB) {
	dropOrdersTableQuery := `DROP TABLE IF EXISTS orders`
	_, err := db.Exec(dropOrdersTableQuery)
	is.NoErr(err)

	dropUsersTableQuery := `DROP TABLE IF EXISTS users`
	_, err = db.Exec(dropUsersTableQuery)
	is.NoErr(err)
}

type User struct {
	ID        int32      `db:"id"`
	Username  string     `db:"username"`
	Email     string     `db:"email"`
	CreatedAt *time.Time `db:"created_at"`
}

// WithName makes updating the username easier
func (u User) WithName(name string) User {
	u.Username = name
	return u
}

func (u User) ToStructuredData() sdk.StructuredData {
	return sdk.StructuredData{
		"id":         u.ID,
		"username":   u.Username,
		"email":      u.Email,
		"created_at": u.CreatedAt.In(time.Now().Location()).Format("2006-01-02 15:04:05"),
	}
}

func (TestTables) Create(is *is.I, db *sqlx.DB) {
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

func (TestTables) InsertUser(is *is.I, db *sqlx.DB, username string) User {
	_, err := db.Exec(`
		INSERT INTO users (username, email) 
		VALUES (?, ?);
	`, username, fmt.Sprint(username, "@example.com"))
	is.NoErr(err)

	var user User
	err = db.QueryRowx(`
		SELECT *
		FROM users
		WHERE id = LAST_INSERT_ID();
	`).StructScan(&user)
	is.NoErr(err)

	return user
}

func (TestTables) DeleteUser(is *is.I, db *sqlx.DB, id int32) {
	_, err := db.Exec(`
		DELETE FROM users
		WHERE id = ?;
	`, id)
	is.NoErr(err)
}

func (TestTables) InsertData(is *is.I, db *sqlx.DB) {
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

func (TestTables) UpdateUser(is *is.I, db *sqlx.DB, user User) {
	_, err := db.Exec(`
		UPDATE users
		SET username = ?, email = ?
		WHERE id = ?;
	`, user.Username, user.Email, user.ID)
	is.NoErr(err)
}
