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
			is.True(strings.Contains(coded.Fix, "make them visible")) // the lighter remedy comes first
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
