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
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func testDestination(ctx context.Context, is *is.I) (sdk.Destination, func()) {
	destination := &Destination{}

	destCfg := map[string]string{
		"dsn": testutils.DSN,
	}

	err := sdk.Util.ParseConfig(ctx,
		destCfg, destination.Config(),
		Connector.NewSpecification().DestinationParams,
	)
	is.NoErr(err)

	is.NoErr(destination.Open(ctx))

	return destination, func() { is.NoErr(destination.Teardown(ctx)) }
}

func TestDestination_OperationSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)

	user1 := testutils.InsertUser(is, db, 1)
	user2 := testutils.InsertUser(is, db, 2)
	user3 := testutils.InsertUser(is, db, 3)

	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	src, cleanSrc := testSourceFromUsers(ctx, is)
	defer cleanSrc()

	rec1 := testutils.ReadAndAssertSnapshot(ctx, is, src, user1)
	rec2 := testutils.ReadAndAssertSnapshot(ctx, is, src, user2)
	rec3 := testutils.ReadAndAssertSnapshot(ctx, is, src, user3)

	// clean table to assert snapshots were written
	testutils.CreateUserTable(is, db)

	written, err := dest.Write(ctx, []opencdc.Record{rec1, rec2, rec3})
	is.NoErr(err)
	is.Equal(written, 3)

	insertedUser1 := testutils.GetUser(is, db, user1.ID)
	insertedUser2 := testutils.GetUser(is, db, user2.ID)
	insertedUser3 := testutils.GetUser(is, db, user3.ID)

	is.Equal("", cmp.Diff(user1, insertedUser1))
	is.Equal("", cmp.Diff(user2, insertedUser2))
	is.Equal("", cmp.Diff(user3, insertedUser3))
}

func TestDestination_OperationCreate(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)
	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	src, cleanSrc := testSourceFromUsers(ctx, is)
	defer cleanSrc()

	user1 := testutils.InsertUser(is, db, 1)
	user2 := testutils.InsertUser(is, db, 2)
	user3 := testutils.InsertUser(is, db, 3)

	rec1 := testutils.ReadAndAssertCreate(ctx, is, src, user1)
	rec2 := testutils.ReadAndAssertCreate(ctx, is, src, user2)
	rec3 := testutils.ReadAndAssertCreate(ctx, is, src, user3)

	// clean table to assert snapshots were written
	testutils.CreateUserTable(is, db)

	written, err := dest.Write(ctx, []opencdc.Record{rec1, rec2, rec3})
	is.NoErr(err)
	is.Equal(written, 3)

	insertedUser1 := testutils.GetUser(is, db, user1.ID)
	insertedUser2 := testutils.GetUser(is, db, user2.ID)
	insertedUser3 := testutils.GetUser(is, db, user3.ID)

	is.Equal("", cmp.Diff(user1, insertedUser1))
	is.Equal("", cmp.Diff(user2, insertedUser2))
	is.Equal("", cmp.Diff(user3, insertedUser3))
}

func TestDestination_OperationUpdate(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)

	user1 := testutils.InsertUser(is, db, 1)
	user2 := testutils.InsertUser(is, db, 2)
	user3 := testutils.InsertUser(is, db, 3)

	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	src, cleanSrc := testSourceFromUsers(ctx, is)
	defer cleanSrc()

	user1Updated := testutils.UpdateUser(is, db, user1.Update())
	user2Updated := testutils.UpdateUser(is, db, user2.Update())
	user3Updated := testutils.UpdateUser(is, db, user3.Update())

	// discard snapshots, we want the updates only
	testutils.ReadAndAssertSnapshot(ctx, is, src, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, src, user2)
	testutils.ReadAndAssertSnapshot(ctx, is, src, user3)

	rec1 := testutils.ReadAndAssertUpdate(ctx, is, src, user1, user1Updated)
	rec2 := testutils.ReadAndAssertUpdate(ctx, is, src, user2, user2Updated)
	rec3 := testutils.ReadAndAssertUpdate(ctx, is, src, user3, user3Updated)

	// clean table to assert snapshots were written
	testutils.CreateUserTable(is, db)

	written, err := dest.Write(ctx, []opencdc.Record{rec1, rec2, rec3})
	is.NoErr(err)
	is.Equal(written, 3)

	insertedUser1 := testutils.GetUser(is, db, user1.ID)
	insertedUser2 := testutils.GetUser(is, db, user2.ID)
	insertedUser3 := testutils.GetUser(is, db, user3.ID)

	is.Equal("", cmp.Diff(user1Updated, insertedUser1))
	is.Equal("", cmp.Diff(user2Updated, insertedUser2))
	is.Equal("", cmp.Diff(user3Updated, insertedUser3))
}

func TestDestination_OperationDelete(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	testutils.CreateUserTable(is, db)

	user1 := testutils.InsertUser(is, db, 1)
	user2 := testutils.InsertUser(is, db, 2)
	user3 := testutils.InsertUser(is, db, 3)

	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	src, cleanSrc := testSourceFromUsers(ctx, is)
	defer cleanSrc()

	testutils.DeleteUser(is, db, user1)
	testutils.DeleteUser(is, db, user2)
	testutils.DeleteUser(is, db, user3)

	// discard snapshots, we want the deletes only
	testutils.ReadAndAssertSnapshot(ctx, is, src, user1)
	testutils.ReadAndAssertSnapshot(ctx, is, src, user2)
	testutils.ReadAndAssertSnapshot(ctx, is, src, user3)

	rec1 := testutils.ReadAndAssertDelete(ctx, is, src, user1)
	rec2 := testutils.ReadAndAssertDelete(ctx, is, src, user2)
	rec3 := testutils.ReadAndAssertDelete(ctx, is, src, user3)

	// reset autoincrement primary key
	testutils.CreateUserTable(is, db)

	// insert users back to assert deletes where done
	testutils.InsertUser(is, db, 1)
	testutils.InsertUser(is, db, 2)
	testutils.InsertUser(is, db, 3)

	written, err := dest.Write(ctx, []opencdc.Record{rec1, rec2, rec3})
	is.NoErr(err)
	is.Equal(written, 3)

	total := testutils.CountUsers(is, db)
	is.Equal(total, 0)
}

func TestDestination_HandleUniqueConflicts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	type User struct {
		ID       int    `gorm:"primaryKey" json:"id" db:"id"`
		Username string `gorm:"type:varchar(100);unique" json:"username" db:"username"`
		Email    string `gorm:"type:varchar(100);uniqueIndex:idx_email_age" json:"email" db:"email"`
		Age      int    `gorm:"uniqueIndex:idx_email_age" json:"age" db:"age"`
	}

	testutils.CreateTables(is, db, &User{})

	initialUsers := []User{
		{ID: 1, Username: "uniqueUser", Email: "initial@example.com", Age: 25},
		{ID: 2, Username: "anotherUser", Email: "another@example.com", Age: 30},
	}
	is.NoErr(db.Create(&initialUsers).Error)

	createRecord := func(id int, username, email string, age int) opencdc.Record {
		return opencdc.Record{
			Operation: opencdc.OperationCreate,
			Metadata:  opencdc.Metadata{"opencdc.collection": "users"},
			Key:       opencdc.StructuredData{"id": id},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"id":       id,
					"username": username,
					"email":    email,
					"age":      age,
				},
			},
		}
	}

	records := []opencdc.Record{
		createRecord(3, "uniqueUser", "new@example.com", 35),
		createRecord(4, "newUser", "initial@example.com", 25),
		createRecord(5, "newUniqueUser", "unique@example.com", 40),
	}

	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()
	written, err := dest.Write(ctx, records)
	is.NoErr(err)
	is.Equal(len(records), written)

	verifyUser := func(username, email string, age int) {
		var user User
		is.NoErr(db.DB.First(&user, "username = ?", username).Error)
		is.Equal(email, user.Email)
		is.Equal(age, user.Age)
	}

	verifyUserComposite := func(email string, age int, expectedUsername string, expectedID int) {
		var user User
		is.NoErr(db.DB.Take(&user, "email = ? AND age = ?", email, age).Error)
		is.Equal(expectedUsername, user.Username)
		is.Equal(expectedID, user.ID)
	}

	verifyUser("uniqueUser", "new@example.com", 35)
	verifyUserComposite("initial@example.com", 25, "newUser", 4)
	verifyUser("newUniqueUser", "unique@example.com", 40)
}

func TestDestination_DeletesWithMultipleKeys(t *testing.T) {
	type MultiKeyTable struct {
		ID       int    `gorm:"primaryKey"`
		Username string `gorm:"primaryKey"`
		Email    string
		Age      int
	}

	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	testutils.CreateTables(is, db, &MultiKeyTable{})

	initialUsers := []MultiKeyTable{
		{ID: 1, Username: "user1", Email: "user1@example.com", Age: 25},
		{ID: 2, Username: "user2", Email: "user2@example.com", Age: 30},
		{ID: 3, Username: "user3", Email: "user3@example.com", Age: 35},
	}
	is.NoErr(db.Create(&initialUsers).Error)

	createDeleteRecord := func(id int, username string) opencdc.Record {
		return opencdc.Record{
			Operation: opencdc.OperationDelete,
			Metadata:  opencdc.Metadata{"opencdc.collection": "multi_key_tables"},
			Key: opencdc.StructuredData{
				"id":       id,
				"username": username,
			},
		}
	}

	records := []opencdc.Record{
		createDeleteRecord(1, "user1"),
		createDeleteRecord(2, "user2"),
	}

	dest, cleanDest := testDestination(ctx, is)
	defer cleanDest()

	written, err := dest.Write(ctx, records)
	is.NoErr(err)
	is.Equal(len(records), written)

	var remainingUsers []MultiKeyTable
	is.NoErr(db.Find(&remainingUsers).Error)
	is.Equal(1, len(remainingUsers))
	is.Equal(3, remainingUsers[0].ID)
	is.Equal("user3", remainingUsers[0].Username)
}
