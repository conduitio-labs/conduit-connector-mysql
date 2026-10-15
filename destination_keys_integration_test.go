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
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func keysTestDelete(table string, key opencdc.Data) opencdc.Record {
	metadata := opencdc.Metadata{}
	metadata.SetCollection(table)
	return opencdc.Record{Operation: opencdc.OperationDelete, Metadata: metadata, Key: key}
}

// Delete keys above 2^53 were round-tripped through float64, so the delete for
// 9007199254740993 removed 9007199254740992 (invariant 6).
func TestDestination_Delete_LargeIntegerKeyIsExact(t *testing.T) {
	for name, key := range map[string]opencdc.Data{
		"structured key": opencdc.StructuredData{"id": int64(9007199254740993)},
		"raw JSON key":   opencdc.RawData(`{"id":9007199254740993}`),
		"raw exponent":   opencdc.RawData(`{"id":9.007199254740993e15}`),
		"raw decimal":    opencdc.RawData(`{"id":9007199254740993.0}`),
		"string":         opencdc.StructuredData{"id": "9007199254740993"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			db := testutils.NewDB(t)

			is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_bigkey").Error)
			is.NoErr(db.Exec("CREATE TABLE dest_bigkey (id BIGINT PRIMARY KEY, v VARCHAR(10))").Error)
			t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_bigkey") })
			is.NoErr(db.Exec("INSERT INTO dest_bigkey VALUES (9007199254740992, 'keep'), (9007199254740993, 'delete')").Error)

			dest, cleanup := testDestination(ctx, is)
			defer cleanup()

			written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_bigkey", key)})
			is.NoErr(err)
			is.Equal(written, 1)

			var ids []int64
			is.NoErr(db.Raw("SELECT id FROM dest_bigkey ORDER BY id").Scan(&ids).Error)
			is.Equal(ids, []int64{9007199254740992})
		})
	}
}

// A delete whose key cannot be matched to the primary key fails with a code
// and deletes nothing; a nil key used to panic.
func TestDestination_Delete_KeyWithoutPrimaryKeyColumn(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keymiss").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_keymiss (id BIGINT PRIMARY KEY, v VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_keymiss") })
	is.NoErr(db.Exec("INSERT INTO dest_keymiss VALUES (1, 'a')").Error)

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	for name, key := range map[string]opencdc.Data{
		"nil key":            nil,
		"other column":       opencdc.StructuredData{"v": "a"},
		"raw key not object": opencdc.RawData(`1`),
		"trailing data":      opencdc.RawData(`{"id":1} {"id":2}`),
		"null value":         opencdc.StructuredData{"id": nil},
		"raw null value":     opencdc.RawData(`{"id":null}`),
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keymiss", key)})
			is.True(errors.Is(err, ErrKeyColumnMissing))
			is.Equal(written, 0)

			var count int64
			is.NoErr(db.Raw("SELECT COUNT(*) FROM dest_keymiss").Scan(&count).Error)
			is.Equal(count, int64(1))
		})
	}
}

// A delete on a table without a primary key fails; once the key is added the
// same destination must see it instead of the cached empty key list.
func TestDestination_Delete_PrimaryKeyAddedLater(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_pklater").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_pklater (id BIGINT NOT NULL, v VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_pklater") })
	is.NoErr(db.Exec("INSERT INTO dest_pklater VALUES (1, 'a'), (2, 'b')").Error)

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	del := keysTestDelete("dest_pklater", opencdc.StructuredData{"id": 1})
	_, err := dest.Write(ctx, []opencdc.Record{del})
	is.True(errors.Is(err, ErrDeleteWithoutPrimaryKey))

	is.NoErr(db.Exec("ALTER TABLE dest_pklater ADD PRIMARY KEY (id)").Error)
	written, err := dest.Write(ctx, []opencdc.Record{del})
	is.NoErr(err)
	is.Equal(written, 1)

	var ids []int64
	is.NoErr(db.Raw("SELECT id FROM dest_pklater ORDER BY id").Scan(&ids).Error)
	is.Equal(ids, []int64{2})
}

// squirrel turns a slice into IN (...): {"a":1,"id":[1,2,3]} deleted three
// rows and {"a":1,"id":[]} was an acked no-op. Key values must be scalars, on
// the single-key and the composite-key path.
func TestDestination_Delete_NonScalarKeyValueFails(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keylist").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_keylist (a BIGINT, id BIGINT, PRIMARY KEY (a, id))").Error)
	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keylist1").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_keylist1 (id BIGINT PRIMARY KEY)").Error)
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS dest_keylist")
		db.Exec("DROP TABLE IF EXISTS dest_keylist1")
	})

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	for name, tc := range map[string]struct {
		table string
		key   opencdc.Data
	}{
		"composite raw list":        {"dest_keylist", opencdc.RawData(`{"a":1,"id":[1,2,3]}`)},
		"composite empty list":      {"dest_keylist", opencdc.RawData(`{"a":1,"id":[]}`)},
		"composite structured list": {"dest_keylist", opencdc.StructuredData{"a": 1, "id": []any{1, 2, 3}}},
		"composite typed list":      {"dest_keylist", opencdc.StructuredData{"a": 1, "id": []int64{1, 2}}},
		"composite object":          {"dest_keylist", opencdc.RawData(`{"a":1,"id":{"x":1}}`)},
		"single raw list":           {"dest_keylist1", opencdc.RawData(`{"id":[1,2]}`)},
		"single structured list":    {"dest_keylist1", opencdc.StructuredData{"id": []any{1, 2}}},
		"single empty list":         {"dest_keylist1", opencdc.RawData(`{"id":[]}`)},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			is.NoErr(db.Exec("DELETE FROM dest_keylist").Error)
			is.NoErr(db.Exec("INSERT INTO dest_keylist VALUES (1, 1), (1, 2), (1, 3)").Error)
			is.NoErr(db.Exec("DELETE FROM dest_keylist1").Error)
			is.NoErr(db.Exec("INSERT INTO dest_keylist1 VALUES (1), (2), (3)").Error)

			written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete(tc.table, tc.key)})
			is.True(errors.Is(err, ErrKeyValueNotScalar))
			is.Equal(written, 0)

			var count int64
			is.NoErr(db.Raw("SELECT COUNT(*) FROM " + tc.table).Scan(&count).Error)
			is.Equal(count, int64(3))
		})
	}
}

// A fraction or a non-numeric string against an integer key column is compared
// as a double: 1.5 matched nothing (an acked no-op) and "abc" would match 0.
// Whole numbers in any form, integer strings and booleans (TINYINT(1) keys)
// still delete their row.
func TestDestination_Delete_IntegerKeyNeedsWholeNumber(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keyfrac").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_keyfrac (id BIGINT PRIMARY KEY, v VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_keyfrac") })

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	reset := func(t *testing.T) {
		is := is.New(t)
		is.NoErr(db.Exec("DELETE FROM dest_keyfrac").Error)
		is.NoErr(db.Exec("INSERT INTO dest_keyfrac VALUES (0, 'zero'), (1, 'one'), (5, 'five')").Error)
	}
	count := func(t *testing.T) int64 {
		var n int64
		is.New(t).NoErr(db.Raw("SELECT COUNT(*) FROM dest_keyfrac").Scan(&n).Error)
		return n
	}

	for name, key := range map[string]opencdc.Data{
		"raw fraction":        opencdc.RawData(`{"id":1.5}`),
		"structured fraction": opencdc.StructuredData{"id": 1.5},
		"string fraction":     opencdc.StructuredData{"id": "9007199254740993.5"},
		"non-numeric string":  opencdc.StructuredData{"id": "abc"},
		"numeric prefix":      opencdc.StructuredData{"id": "1abc"},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			reset(t)
			written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keyfrac", key)})
			is.True(errors.Is(err, ErrKeyValueTypeMismatch))
			is.Equal(written, 0)
			is.Equal(count(t), int64(3))
		})
	}

	for name, key := range map[string]opencdc.Data{
		"whole float":    opencdc.StructuredData{"id": float64(5)},
		"raw exponent":   opencdc.RawData(`{"id":5e0}`),
		"integer string": opencdc.StructuredData{"id": "5"},
		"bool":           opencdc.StructuredData{"id": true}, // MySQL: true is 1
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			reset(t)
			written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keyfrac", key)})
			is.NoErr(err)
			is.Equal(written, 1)
			is.Equal(count(t), int64(2))
		})
	}
}
