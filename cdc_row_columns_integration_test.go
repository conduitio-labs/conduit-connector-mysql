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
	"errors"
	"strings"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/matryer/is"
)

// Regression test (#231, invariant 6): a binlog event written before a column
// was dropped, read after it, has more values than the table has columns. The
// first values were mapped to the remaining columns and the rest dropped, so
// after a dropped middle column every later value landed in the wrong column,
// silently. The source must halt with a stable code instead.
func TestCDCIterator_RowWithMoreValuesThanColumnsHalts(t *testing.T) {
	for name, drop := range map[string]string{
		"middle column dropped": "username",
		"last column dropped":   "created_at",
	} {
		t.Run(name, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			db := testutils.NewDB(t)
			testutils.CreateUserTable(is, db)
			exec := newSession(ctx, t, is)

			iterator, teardown := testCdcIterator(ctx, t, is)
			exec.insertUsers(1, 1)
			_, pos := readIDs(ctx, is, iterator, 1)
			teardown()

			// Written while the connector is down; read after the ALTER ran.
			exec.insertUsers(2, 2)
			exec("ALTER TABLE users DROP COLUMN " + drop)

			iterator, _ = testCdcIteratorAtPosition(ctx, t, is, pos)
			err := readUntilError(ctx, iterator)
			var coded *Error
			if !errors.As(err, &coded) {
				t.Fatalf("want a %s halt, got %v", CodeRowColumnsMismatch, err)
			}
			is.Equal(coded.Code, CodeRowColumnsMismatch)
			is.True(strings.Contains(coded.Message, "has 4 values, but the table now has 3 columns"))
			is.True(strings.Contains(coded.Fix, "make them visible")) // hidden columns first
			restart, reset := strings.Index(coded.Fix, "Restart the pipeline first"), strings.Index(coded.Fix, "reset the pipeline's position")
			is.True(restart >= 0 && reset > restart) // restart is offered before the re-snapshot
			is.True(errors.Is(iterator.Teardown(ctx), ErrRowColumnsMismatch))
		})
	}
}

// Generated (virtual and stored) and INVISIBLE columns are in both the row
// image and the table's column list, so the stricter check must not halt on
// them.
func TestCDCIterator_GeneratedAndInvisibleColumnsDoNotHalt(t *testing.T) {
	for name, ddl := range map[string]string{
		"virtual generated": "ALTER TABLE users ADD COLUMN gen_v BIGINT GENERATED ALWAYS AS (CHAR_LENGTH(username)) VIRTUAL",
		"stored generated":  "ALTER TABLE users ADD COLUMN gen_s BIGINT GENERATED ALWAYS AS (CHAR_LENGTH(username)) STORED",
		"invisible":         "ALTER TABLE users ADD COLUMN hidden INT NULL INVISIBLE",
	} {
		t.Run(name, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			db := testutils.NewDB(t)
			testutils.CreateUserTable(is, db)
			exec := newSession(ctx, t, is)
			exec(ddl)

			iterator, teardown := testCdcIterator(ctx, t, is)
			defer teardown()
			exec.insertUsers(1, 2)
			exec("UPDATE users SET username = 'renamed' WHERE id = 1")
			exec("DELETE FROM users WHERE id = 2")

			ids, _ := readIDs(ctx, is, iterator, 4)
			is.Equal(ids, []int64{1, 2, 1, 2})
			assertNoMoreIDs(ctx, t, iterator)
		})
	}
}

// Regression test (#232 review): canal refreshes its cached columns only for
// DDL its parser understands. ADD COLUMN ... INVISIBLE is skipped, so the
// next event looked one column too wide and the source halted, although the
// table's current columns matched it (a restart then delivered it). The
// source re-reads the columns once on a mismatch and delivers.
func TestCDCIterator_DDLCanalCannotParseDoesNotHalt(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	iterator, teardown := testCdcIterator(ctx, t, is)
	defer teardown()
	exec.insertUsers(1, 1)
	ids, _ := readIDs(ctx, is, iterator, 1) // canal caches the columns
	is.Equal(ids, []int64{1})

	exec("ALTER TABLE users ADD COLUMN hidden INT NULL INVISIBLE")
	exec("INSERT INTO users (id, username, email, created_at, hidden) VALUES (2, 'user-2', 'user-2@example.com', NOW(), 7)")
	exec.insertUsers(3, 3)

	ids, _ = readIDs(ctx, is, iterator, 2)
	is.Equal(ids, []int64{2, 3})
	assertNoMoreIDs(ctx, t, iterator)
}

// With an unsigned column in the stale cache, canal has already converted
// values by the stale column indexes, so the re-read columns are not used and
// the event halts; the cache is fresh by then, so a restart delivers it.
func TestCDCIterator_StaleColumnsWithUnsignedHaltThenRestartDelivers(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)
	exec("ALTER TABLE users ADD COLUMN counter INT UNSIGNED NULL")

	iterator, _ := testCdcIterator(ctx, t, is)
	exec.insertUsers(1, 1)
	_, pos := readIDs(ctx, is, iterator, 1)

	exec("ALTER TABLE users ADD COLUMN hidden INT NULL INVISIBLE")
	exec.insertUsers(2, 2)
	err := readUntilError(ctx, iterator)
	is.True(errors.Is(err, ErrRowColumnsMismatch))
	_ = iterator.Teardown(ctx)

	iterator, teardown := testCdcIteratorAtPosition(ctx, t, is, pos)
	defer teardown()
	ids, _ := readIDs(ctx, is, iterator, 1)
	is.Equal(ids, []int64{2})
	assertNoMoreIDs(ctx, t, iterator)
}

// A generated invisible primary key hidden from SHOW COLUMNS
// (show_gipk_in_create_table_and_information_schema=OFF) is in every row
// image but not in the connector's column list, so its values cannot be
// placed: the source halts, by design, even after re-reading the columns.
func TestCDCIterator_HiddenGeneratedInvisiblePrimaryKeyHalts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	exec := newSession(ctx, t, is)
	exec("DROP TABLE IF EXISTS users")
	exec("SET SESSION sql_generate_invisible_primary_key = ON")
	exec("CREATE TABLE users (id BIGINT NOT NULL, username VARCHAR(255), email VARCHAR(255), created_at DATETIME(3))")
	exec("SET SESSION sql_generate_invisible_primary_key = OFF")
	exec("SET GLOBAL show_gipk_in_create_table_and_information_schema = OFF")
	t.Cleanup(func() { exec("SET GLOBAL show_gipk_in_create_table_and_information_schema = ON") })

	iterator, _ := testCdcIterator(ctx, t, is)
	exec.insertUsers(1, 1)
	err := readUntilError(ctx, iterator)
	var coded *Error
	is.True(errors.As(err, &coded))
	is.Equal(coded.Code, CodeRowColumnsMismatch)
	is.True(strings.Contains(coded.Fix, "show_gipk_in_create_table_and_information_schema = ON"))
	_ = iterator.Teardown(ctx)
}
