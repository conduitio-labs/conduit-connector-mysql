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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

// A global read lock must be released by UNLOCK TABLES on the same session.
// lockTables ran both through the sql.DB pool, so when the connection that took
// the lock was busy the UNLOCK went to another connection, did nothing, and
// every table stayed read-locked until the pool closed.
func TestLockTables_UnlockReleasesLockWhenPoolIsBusy(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)

	unlock, err := lockTables(ctx, db.SqlxDB, []string{"users"})
	is.NoErr(err)

	// Occupy the pool the way concurrent work does: the connection that took
	// the lock (the only idle one) is now in use.
	busy, err := db.SqlxDB.Connx(ctx)
	is.NoErr(err)
	defer busy.Close()

	is.NoErr(unlock())

	// A writer on its own connection must now get through.
	other := testutils.NewDB(t)
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = other.SqlxDB.ExecContext(writeCtx,
		"INSERT INTO users (username, email, created_at) VALUES ('u', 'u@example.com', NOW())")
	is.NoErr(err) // blocked by a leaked lock before the fix
}

// An error after the lock is taken (here: a key column that does not exist)
// returns no iterator, so nothing else can release the lock. It used to stay
// held, blocking every writer.
func TestNewCombinedIterator_ErrorAfterLockReleasesLock(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)

	config, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)
	config.ParseTime = true

	_, err = newCombinedIterator(ctx, combinedIteratorConfig{
		db:                  db.SqlxDB,
		tableKeys:           common.TableKeys{"users": {"no_such_column"}},
		database:            "meroxadb",
		serverID:            testutils.ServerID,
		mysqlConfig:         config,
		disableCanalLogging: true,
		snapshotEnabled:     true,
	})
	is.True(err != nil)

	other := testutils.NewDB(t)
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = other.SqlxDB.ExecContext(writeCtx,
		"INSERT INTO users (username, email, created_at) VALUES ('u', 'u@example.com', NOW())")
	is.NoErr(err)
}

// Calling the release function twice is harmless.
func TestLockTables_UnlockIsIdempotent(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)

	unlock, err := lockTables(ctx, db.SqlxDB, []string{"users"})
	is.NoErr(err)
	is.NoErr(unlock())
	is.NoErr(unlock())
}

// The UNLOCK-failure branch: the lock session is killed, so UNLOCK TABLES fails.
// The error is reported (once, and the same on repeat calls), the killed
// connection is not returned to the pool, and the lock is gone with the session.
func TestLockTables_UnlockFailureIsReportedAndRecovered(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	setup := testutils.NewDB(t)
	testutils.CreateUserTable(is, setup)

	// A pool with exactly one connection, so lockTables must use the one whose
	// id we read first.
	pool := testutils.NewDB(t).SqlxDB
	pool.SetMaxOpenConns(1)
	var connID int64
	is.NoErr(pool.GetContext(ctx, &connID, "SELECT CONNECTION_ID()"))

	unlock, err := lockTables(ctx, pool, []string{"users"})
	is.NoErr(err)

	// Kill the lock session from another connection.
	_, err = setup.SqlxDB.ExecContext(ctx, fmt.Sprintf("KILL CONNECTION %d", connID))
	is.NoErr(err)

	err = unlock()
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "failed to unlock tables"))
	is.Equal(unlock().Error(), err.Error()) // idempotent: reports the same failure, does not re-run

	// The lock died with the session.
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err = setup.SqlxDB.ExecContext(writeCtx,
		"INSERT INTO users (username, email, created_at) VALUES ('u', 'u@example.com', NOW())")
	is.NoErr(err)

	// And the pool is usable: the dead connection was discarded, not reused.
	var one int
	is.NoErr(pool.GetContext(ctx, &one, "SELECT 1"))
	is.Equal(one, 1)
}
