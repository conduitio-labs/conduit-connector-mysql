// Copyright © 2026 Meroxa, Inc.
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
	"math"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// TestCDCIterator_ResumeAcrossSixDigitBinlogBoundary is the end-to-end
// regression test for #238, against a real server. MySQL pads the binlog
// sequence number to at least six digits and keeps counting, so the file
// after binlog.999999 is binlog.1000000, which sorts before it as text. The
// test moves the server's binlog to 999999 (RESET MASTER TO), rotates past
// the boundary, and resumes from positions in the six-digit file.
//
// It resets the server's binary logs: run it only against a test server.
func TestCDCIterator_ResumeAcrossSixDigitBinlogBoundary(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	resetBinlogTo(ctx, is, db, 999999)
	testutils.CreateUserTable(is, db)

	iterator, teardown := testCdcIterator(ctx, t, is)
	user1 := testutils.InsertUser(is, db, 1)
	saved := testutils.ReadAndAssertCreate(ctx, is, iterator, user1).Position
	teardown()

	pos, err := common.ParseSDKPosition(saved)
	is.NoErr(err)
	is.True(strings.HasSuffix(pos.CdcPosition.Name, ".999999")) // the record is in the six-digit file

	_, err = db.SqlxDB.ExecContext(ctx, "FLUSH BINARY LOGS")
	is.NoErr(err)
	user2 := testutils.InsertUser(is, db, 2)

	t.Run("resume from the saved event", func(t *testing.T) {
		is := is.New(t)
		iterator, teardown := testCdcIteratorAtPosition(ctx, t, is, saved)
		defer teardown()

		rec := readWithin(ctx, is, iterator, user2)
		next, err := common.ParseSDKPosition(rec.Position)
		is.NoErr(err)
		is.True(strings.HasSuffix(next.CdcPosition.Name, ".1000000")) // the server rotated past the boundary
	})

	// The saved event is not in its file: its offset lies past every event
	// binlog.999999 holds, as after a failover without GTIDs to a server
	// whose file of the same name is shorter. The resume then looks for the
	// saved event until a later position shows it will not come. That later
	// position is the rotation to binlog.1000000; with names compared as
	// text it read as earlier, and every later event was dropped as already
	// delivered.
	t.Run("resume when the saved event is not in its file", func(t *testing.T) {
		is := is.New(t)
		missing := *pos.CdcPosition
		missing.Pos = math.MaxUint32 - 1
		iterator, teardown := testCdcIteratorAtPosition(ctx, t, is, missing.ToSDKPosition())
		defer teardown()

		readWithin(ctx, is, iterator, user2)
	})
}

// readWithin reads one record, which must be the creation of user, failing
// after a bound instead of waiting for a record that a resume dropped.
func readWithin(ctx context.Context, is *is.I, iterator common.Iterator, user testutils.User) opencdc.Record {
	is.Helper()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return testutils.ReadAndAssertCreate(ctx, is, iterator, user)
}

// resetBinlogTo deletes the server's binary logs and starts a new one
// numbered n: RESET BINARY LOGS AND GTIDS TO on MySQL 8.4 and later, RESET
// MASTER TO before.
func resetBinlogTo(ctx context.Context, is *is.I, db testutils.DB, n int) {
	is.Helper()
	// Not a preparable statement, so the number is formatted in.
	_, err := db.SqlxDB.ExecContext(ctx, fmt.Sprintf("RESET BINARY LOGS AND GTIDS TO %d", n))
	if err != nil {
		_, err = db.SqlxDB.ExecContext(ctx, fmt.Sprintf("RESET MASTER TO %d", n))
	}
	is.NoErr(err)
}
