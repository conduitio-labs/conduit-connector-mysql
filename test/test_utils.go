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
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/gookit/goutil/dump"
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

func ReadAndAssertInsert(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) sdk.Record {
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, sdk.OperationCreate)

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	IsDataEqual(is, rec.Key, sdk.StructuredData{
		"id":     user.ID,
		"table":  "users",
		"action": "insert",
	})

	IsDataEqual(is, rec.Payload.After, user.ToStructuredData())

	return rec
}

func ReadAndAssertUpdate(
	ctx context.Context, is *is.I,
	iterator common.Iterator, prev, next User,
) {
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, sdk.OperationUpdate)
	is.Equal(rec.Metadata["mysql.action"], "update")

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	IsDataEqual(is, rec.Payload.Before, prev.ToStructuredData())
	IsDataEqual(is, rec.Payload.After, next.ToStructuredData())
}

func ReadAndAssertDelete(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) {
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, sdk.OperationDelete)
	is.Equal(rec.Metadata["mysql.action"], "delete")

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	IsDataEqual(is, rec.Key, sdk.StructuredData{
		"id":     user.ID,
		"table":  "users",
		"action": "delete",
	})
}

func IsDataEqual(is *is.I, a, b sdk.Data) {
	if a == nil && b == nil {
		return
	}

	if a == nil || b == nil {
		is.Fail() // one of the data is nil
	}

	equal, err := JSONBytesEqual(a.Bytes(), b.Bytes())
	is.NoErr(err)

	aS, ok_aS := a.(sdk.StructuredData)
	bS, ok_bS := b.(sdk.StructuredData)

	// dump structured datas for easier debugging
	if !equal {
		if ok_aS {
			dump.P(aS)
		} else {
			fmt.Println(string(a.Bytes()))
		}
		if ok_bS {
			dump.P(bS)
		} else {
			fmt.Println(string(b.Bytes()))
		}
	}

	// if both are structured, let's offer a better error

	if ok_aS && ok_bS {
		for key, val := range aS {
			is.Equal(bS[key], val)
		}
		return
	}

	// otherwise, simply fail the test

	is.True(equal) // compared datas are not equal

}

// JSONBytesEqual compares the JSON in two byte slices.
func JSONBytesEqual(a, b []byte) (bool, error) {
	var j, j2 interface{}
	if err := json.Unmarshal(a, &j); err != nil {
		return false, fmt.Errorf("failed to unmarshal first JSON: %w", err)
	}
	if err := json.Unmarshal(b, &j2); err != nil {
		return false, fmt.Errorf("failed to unmarshal second JSON: %w", err)
	}

	return reflect.DeepEqual(j2, j), nil
}

func ReadAndAssertSnapshot(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) {
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, sdk.OperationSnapshot)

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	IsDataEqual(is, rec.Key, sdk.StructuredData{
		"table": common.TableName("users"),
		"key":   "id",
		"value": user.ID,
	})

	IsDataEqual(is, rec.Payload.After, user.ToStructuredData())
}

func AssertUserSnapshot(is *is.I, user User, rec sdk.Record) {
	is.Equal(rec.Operation, sdk.OperationSnapshot)

	col, err := rec.Metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	IsDataEqual(is, rec.Key, sdk.StructuredData{
		"key":   "id",
		"value": user.ID,
		"table": common.TableName("users"),
	})

	IsDataEqual(is, rec.Payload.After, user.ToStructuredData())
}
