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
	"os"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-sql-driver/mysql"
	"github.com/google/go-cmp/cmp"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

const DSN = "root:meroxaadmin@tcp(127.0.0.1:3306)/meroxadb?parseTime=true"

var ServerID = "1"

func Connection(t *testing.T) *sqlx.DB {
	is := is.New(t)
	db, err := sqlx.Open("mysql", DSN)
	is.NoErr(err)

	t.Cleanup(func() {
		is.NoErr(db.Close())
	})

	return db
}

func TestContext(t *testing.T) context.Context {
	writer := zerolog.NewTestWriter(t)
	consoleWriter := zerolog.ConsoleWriter{
		Out:        writer,
		PartsOrder: []string{"level", "message"},
	}

	traceLog := os.Getenv("TRACE") == "true"
	level := zerolog.InfoLevel
	if traceLog {
		level = zerolog.TraceLevel
	}
	logger := zerolog.New(consoleWriter).Level(level)

	return logger.WithContext(context.Background())
}

var TableSortCols = common.TableSortColumns{
	"users": "id",
}

type User struct {
	ID        int64     `db:"id"`
	Username  string    `db:"username"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
}

func (u User) Update() User {
	u.Username = fmt.Sprintf("%v-updated", u.Username)
	u.Email = fmt.Sprintf("%v-updated@example.com", u.Email)
	return u
}

func (u User) ToStructuredData() opencdc.StructuredData {
	return opencdc.StructuredData{
		"id":         u.ID,
		"username":   u.Username,
		"email":      u.Email,
		"created_at": u.CreatedAt.UTC(),
	}
}

func RecreateUsersTable(is *is.I, db *sqlx.DB) {
	_, err := db.Exec(`DROP TABLE IF EXISTS users`)
	is.NoErr(err)

	_, err = db.Exec(`
	CREATE TABLE users (
		id BIGINT PRIMARY KEY,
		username VARCHAR(255) NOT NULL,
		email VARCHAR(255) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`)
	is.NoErr(err)
}

func InsertUser(is *is.I, db *sqlx.DB, userID int) User {
	username := fmt.Sprint("user-", userID)
	email := fmt.Sprint(username, "@example.com")

	_, err := db.Exec(`
		INSERT INTO users (id, username, email) 
		VALUES (?, ?, ?);
	`, userID, username, email)
	is.NoErr(err)

	var user User
	err = db.QueryRowx(`
		SELECT *
		FROM users
		WHERE id = ?;
	`, userID).StructScan(&user)
	is.NoErr(err)

	return user
}

func GetUser(is *is.I, db *sqlx.DB, userID int64) User {
	var user User
	err := db.QueryRowx(`
		SELECT *
		FROM users
		WHERE id = ?;
	`, userID).StructScan(&user)
	is.NoErr(err)

	return user
}

func UpdateUser(is *is.I, db *sqlx.DB, user User) User {
	_, err := db.Exec(`
		UPDATE users
		SET username = ?, email = ?
		WHERE id = ?;
	`, user.Username, user.Email, user.ID)
	is.NoErr(err)

	return user
}

func DeleteUser(is *is.I, db *sqlx.DB, user User) {
	_, err := db.Exec(`
		DELETE FROM users
		WHERE id = ?;
	`, user.ID)
	is.NoErr(err)
}

func CountUsers(is *is.I, db *sqlx.DB) int {
	var count struct {
		Total int `db:"total"`
	}

	err := db.QueryRowx("SELECT count(*) as total FROM users").StructScan(&count)
	is.NoErr(err)

	return count.Total
}

func ReadAndAssertCreate(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) opencdc.Record {
	is.Helper()
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationCreate)

	assertMetadata(is, rec.Metadata)

	IsDataEqual(is, rec.Key, opencdc.StructuredData{"id": user.ID})
	IsDataEqual(is, rec.Payload.After, user.ToStructuredData())

	return rec
}

func ReadAndAssertUpdate(
	ctx context.Context, is *is.I,
	iterator common.Iterator, prev, next User,
) opencdc.Record {
	is.Helper()
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationUpdate)

	assertMetadata(is, rec.Metadata)

	IsDataEqual(is, rec.Key, opencdc.StructuredData{"id": prev.ID})
	IsDataEqual(is, rec.Key, opencdc.StructuredData{"id": next.ID})

	IsDataEqual(is, rec.Payload.Before, prev.ToStructuredData())
	IsDataEqual(is, rec.Payload.After, next.ToStructuredData())

	return rec
}

func ReadAndAssertDelete(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) opencdc.Record {
	is.Helper()

	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationDelete)

	assertMetadata(is, rec.Metadata)

	IsDataEqual(is, rec.Key, opencdc.StructuredData{"id": user.ID})

	return rec
}

func IsDataEqual(is *is.I, a, b opencdc.Data) {
	is.Helper()
	is.Equal("", cmp.Diff(a, b))
}

func ReadAndAssertSnapshot(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) opencdc.Record {
	is.Helper()
	rec, err := iterator.Read(ctx)
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, rec.Position))

	AssertUserSnapshot(is, user, rec)
	return rec
}

func AssertUserSnapshot(is *is.I, user User, rec opencdc.Record) {
	is.Helper()
	is.Equal(rec.Operation, opencdc.OperationSnapshot)

	assertMetadata(is, rec.Metadata)

	IsDataEqual(is, rec.Key, opencdc.StructuredData{"id": user.ID})
	IsDataEqual(is, rec.Payload.After, user.ToStructuredData())
}

func assertMetadata(is *is.I, metadata opencdc.Metadata) {
	col, err := metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	is.Equal(metadata[common.ServerIDKey], ServerID)
}

func NewCanal(ctx context.Context, is *is.I) *canal.Canal {
	is.Helper()

	config, err := mysql.ParseDSN(DSN)
	is.NoErr(err)

	canal, err := common.NewCanal(ctx, common.CanalConfig{
		Config:         config,
		Tables:         []string{"users"},
		DisableLogging: true,
	})
	is.NoErr(err)

	return canal
}
