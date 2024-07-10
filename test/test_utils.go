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

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func Connection(is *is.I) *sqlx.DB {
	db, err := sqlx.Open("mysql", "root:meroxaadmin@tcp(127.0.0.1:3306)/meroxadb?parseTime=true")
	is.NoErr(err)

	return db
}

func TestContext(t *testing.T) context.Context {
	logger := zerolog.New(zerolog.NewTestWriter(t))
	return logger.WithContext(context.Background())
}

var totalRowsPerTabl = 50

var TableKeys = map[common.TableName]common.PrimaryKeyName{
	"users": "id",
}

type User struct {
	ID        int    `db:"id"`
	Username  string `db:"username"`
	Email     string `db:"email"`
	CreatedAt string `db:"created_at"`
}

func (u User) Update() User {
	u.Username = fmt.Sprintf("%v-updated", u.Username)
	u.Email = fmt.Sprintf("%v-updated@example.com", u.Email)
	return u
}

func (u User) ToStructuredData() sdk.StructuredData {
	return sdk.StructuredData{
		"id":         u.ID,
		"username":   u.Username,
		"email":      u.Email,
		"created_at": u.CreatedAt,
	}
}

type UsersTable struct{}

func (UsersTable) Recreate(is *is.I, db *sqlx.DB) {
	_, err := db.Exec(`DROP TABLE IF EXISTS users`)
	is.NoErr(err)

	_, err = db.Exec(`
	CREATE TABLE users (
		id INT AUTO_INCREMENT PRIMARY KEY,
		username VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)
	is.NoErr(err)
}

func (UsersTable) Insert(is *is.I, db *sqlx.DB, username string) User {
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

func (UsersTable) Update(is *is.I, db *sqlx.DB, user User) User {
	_, err := db.Exec(`
		UPDATE users
		SET username = ?, email = ?
		WHERE id = ?;
	`, user.Username, user.Email, user.ID)
	is.NoErr(err)

	return user
}

func (UsersTable) Delete(is *is.I, db *sqlx.DB, user User) {
	_, err := db.Exec(`
		DELETE FROM users
		WHERE id = ?;
	`, user.ID)
	is.NoErr(err)
}
