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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-sql-driver/mysql"
	"github.com/google/go-cmp/cmp"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	gormschema "gorm.io/gorm/schema"
)

const DSN = "root:meroxaadmin@tcp(127.0.0.1:3306)/meroxadb"

var ServerID = "1"

// DB is a gorm wrapper that also holds a sqlx DB. Iterators
// don't manage sql connections, so we need to store them somehow.
type DB struct {
	*gorm.DB
	SqlxDB *sqlx.DB
}

func NewDB(t *testing.T) DB {
	is := is.New(t)

	// Individual iterators assume that parseTime has already been configured to true, so
	// they have no knowledge whether that has actually been the case.
	// We might want in the future to run many more tests using the Source itself, so that
	// we don't have to do this dance.

	dsnWithParseTime := DSN + "?parseTime=true"

	db, err := gorm.Open(gormmysql.Open(dsnWithParseTime), &gorm.Config{
		Logger: logger.Discard,
	})
	is.NoErr(err)

	sqlDB, err := db.DB()
	is.NoErr(err)
	sqlxDB := sqlx.NewDb(sqlDB, "mysql")

	t.Cleanup(func() {
		sqlxDB.Close()
	})

	return DB{DB: db, SqlxDB: sqlxDB}
}

func TableName(is *is.I, db DB, model any) string {
	stmt := gorm.Statement{DB: db.DB}
	err := stmt.Parse(model)
	is.NoErr(err)

	s, err := gormschema.Parse(model, &sync.Map{}, gormschema.NamingStrategy{})
	is.NoErr(err)

	return s.Table
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

var TablePrimaryKeys = map[string]common.PrimaryKeys{
	"users": {"id"},
}

type User struct {
	ID        int64     `db:"id"`
	Username  string    `db:"username"`
	Email     string    `db:"email"`
	CreatedAt time.Time `db:"created_at"`
}

var (
	userPayloadSchema = AvroSchema{
		Name: "mysql.users_payload",
		Type: "record",
		Fields: []AvroSchemaField{
			{Name: "id", Type: "long"},
			{Name: "username", Type: "string"},
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "string"},
		},
	}
	userKeySchema = AvroSchema{
		Name:   "mysql.users_key",
		Type:   "record",
		Fields: []AvroSchemaField{{Name: "id", Type: "long"}},
	}
)

type AvroSchema struct {
	Name   string            `json:"name"`
	Type   string            `json:"type"`
	Fields []AvroSchemaField `json:"fields"`
}

type AvroSchemaField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (u User) Update() User {
	u.Username = fmt.Sprintf("%v-updated", u.Username)
	u.Email = fmt.Sprintf("%v-updated@example.com", u.Email)
	return u
}

func (u User) StructuredData() opencdc.StructuredData {
	return opencdc.StructuredData{
		"id":         u.ID,
		"username":   u.Username,
		"email":      u.Email,
		"created_at": u.CreatedAt.UTC(),
	}
}

func RecreateUsersTable(is *is.I, db DB) {
	is.NoErr(db.Migrator().DropTable(&User{}))
	is.NoErr(db.AutoMigrate(&User{}))
}

func CreateUser(userID int) *User {
	username := fmt.Sprint("user-", userID)
	email := fmt.Sprint(username, "@example.com")

	// That's easier to work with pointer during
	// multiple rows insertion/modification
	return &User{
		ID:       int64(userID),
		Username: username,
		Email:    email,
	}
}

func InsertUser(is *is.I, db DB, userID int) User {
	user := CreateUser(userID)

	err := db.Create(user).Error
	is.NoErr(err)

	return *user
}

func GetUser(is *is.I, db DB, userID int64) User {
	var user User
	err := db.First(&user, userID).Error
	is.NoErr(err)

	return user
}

func UpdateUser(is *is.I, db DB, user User) User {
	err := db.Model(&user).Updates(User{Username: user.Username, Email: user.Email}).Error
	is.NoErr(err)

	return user
}

func DeleteUser(is *is.I, db DB, user User) {
	err := db.Delete(&user).Error
	is.NoErr(err)
}

func CountUsers(is *is.I, db DB) int {
	var count int64
	err := db.Model(&User{}).Count(&count).Error
	is.NoErr(err)

	return int(count)
}

func ReadAndAssertCreate(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) opencdc.Record {
	is.Helper()
	recs, err := iterator.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1)
	rec := recs[0]
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationCreate)

	assertMetadata(ctx, is, rec.Metadata)

	isDataEqual(is, rec.Key, opencdc.StructuredData{"id": user.ID})
	isDataEqual(is, rec.Payload.After, user.StructuredData())

	return rec
}

func ReadAndAssertUpdate(
	ctx context.Context, is *is.I,
	iterator common.Iterator, prev, next User,
) opencdc.Record {
	is.Helper()
	recs, err := iterator.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1)
	rec := recs[0]
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationUpdate)

	assertMetadata(ctx, is, rec.Metadata)

	isDataEqual(is, rec.Key, opencdc.StructuredData{"id": prev.ID})
	isDataEqual(is, rec.Key, opencdc.StructuredData{"id": next.ID})

	isDataEqual(is, rec.Payload.Before, prev.StructuredData())
	isDataEqual(is, rec.Payload.After, next.StructuredData())

	return rec
}

func ReadAndAssertDelete(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) opencdc.Record {
	is.Helper()

	recs, err := iterator.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1)
	rec := recs[0]
	is.NoErr(iterator.Ack(ctx, rec.Position))

	is.Equal(rec.Operation, opencdc.OperationDelete)

	assertMetadata(ctx, is, rec.Metadata)

	isDataEqual(is, rec.Key, opencdc.StructuredData{"id": user.ID})

	return rec
}

func isDataEqual(is *is.I, actual, expected any) {
	is.Equal("", cmp.Diff(actual, expected)) // actual (-) != expected (+)
}

func ReadAndAssertSnapshot(
	ctx context.Context, is *is.I,
	iterator common.Iterator, user User,
) opencdc.Record {
	is.Helper()
	recs, err := iterator.ReadN(ctx, 1)
	is.NoErr(err)
	is.True(len(recs) == 1)
	is.NoErr(iterator.Ack(ctx, recs[0].Position))

	AssertUserSnapshot(ctx, is, user, recs[0])
	return recs[0]
}

func AssertUserSnapshot(ctx context.Context, is *is.I, user User, rec opencdc.Record) {
	is.Helper()
	is.Equal(rec.Operation, opencdc.OperationSnapshot)

	assertMetadata(ctx, is, rec.Metadata)

	isDataEqual(is, rec.Key, opencdc.StructuredData{"id": user.ID})
	isDataEqual(is, rec.Payload.After, user.StructuredData())
}

func assertMetadata(ctx context.Context, is *is.I, metadata opencdc.Metadata) {
	col, err := metadata.GetCollection()
	is.NoErr(err)
	is.Equal(col, "users")

	is.Equal(metadata[common.ServerIDKey], ServerID)

	assertSchema(ctx, is, metadata)
}

func assertSchema(ctx context.Context, is *is.I, metadata opencdc.Metadata) {
	{ // payload schema
		ver, err := metadata.GetPayloadSchemaVersion()
		is.NoErr(err)
		sub, err := metadata.GetPayloadSchemaSubject()
		is.NoErr(err)

		s, err := schema.Get(ctx, sub, ver)
		is.NoErr(err)

		var actualSchema AvroSchema
		is.NoErr(json.Unmarshal(s.Bytes, &actualSchema))

		isDataEqual(is, actualSchema, userPayloadSchema)
	}

	{ // key schema
		ver, err := metadata.GetKeySchemaVersion()
		is.NoErr(err)
		sub, err := metadata.GetKeySchemaSubject()
		is.NoErr(err)

		s, err := schema.Get(ctx, sub, ver)
		is.NoErr(err)

		var actualSchema AvroSchema
		is.NoErr(json.Unmarshal(s.Bytes, &actualSchema))

		isDataEqual(is, actualSchema, userKeySchema)
	}
}

func newCanal(ctx context.Context, is *is.I, tablename string) *canal.Canal {
	is.Helper()

	config, err := mysql.ParseDSN(DSN)
	is.NoErr(err)

	canal, err := common.NewCanal(ctx, common.CanalConfig{
		Config:         config,
		Tables:         []string{tablename},
		DisableLogging: true,
	})
	is.NoErr(err)

	return canal
}

func TriggerRowInsertEvent(
	ctx context.Context, is *is.I, tablename string, trigger func(),
) *canal.RowsEvent {
	is.Helper()

	c := newCanal(ctx, is, tablename)
	defer c.Close()

	rowsChan := make(chan *canal.RowsEvent)
	doneChan := make(chan struct{})
	defer close(doneChan)

	handler := &testEventHandler{
		rowsChan: rowsChan,
		doneChan: doneChan,
	}
	c.SetEventHandler(handler)

	pos, err := c.GetMasterPos()
	is.NoErr(err)

	go func() {
		if err := c.RunFrom(pos); err != nil {
			is.NoErr(err)
		}
	}()

	trigger()

	var rowsEvent *canal.RowsEvent
	select {
	case rowsEvent = <-rowsChan:
	case <-time.After(1 * time.Second):
		is.Fail()
	}

	return rowsEvent
}

type testEventHandler struct {
	canal.DummyEventHandler
	rowsChan chan *canal.RowsEvent
	doneChan chan struct{}
}

func (h *testEventHandler) OnRow(e *canal.RowsEvent) error {
	select {
	case <-h.doneChan:
		return nil
	case h.rowsChan <- e:
		return nil
	}
}

func (h *testEventHandler) String() string {
	return "testEventHandler"
}
