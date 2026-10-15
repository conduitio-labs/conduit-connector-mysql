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

// MySQL compares a number with a string column as a double, so a numeric key
// 1000 for a VARCHAR primary key deleted '1000', '1e3', '01000', '1000.0' and
// ' 1000'. Numbers are sent as their decimal text to string key columns.
func TestDestination_Delete_NumericKeyOnStringColumn(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_keystr") })

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	for name, tc := range map[string]struct {
		composite bool
		key       opencdc.Data
		deleted   string
	}{
		"raw integer":       {false, opencdc.RawData(`{"id":1000}`), "1000"},
		"raw exponent":      {false, opencdc.RawData(`{"id":1e3}`), "1e3"},
		"structured int64":  {false, opencdc.StructuredData{"id": int64(1000)}, "1000"},
		"structured float":  {false, opencdc.StructuredData{"id": float64(1000)}, "1000"},
		"string":            {false, opencdc.StructuredData{"id": "01000"}, "01000"},
		"composite raw":     {true, opencdc.RawData(`{"a":1,"id":1000}`), "1000"},
		"composite literal": {true, opencdc.RawData(`{"a":1,"id":1000.0}`), "1000.0"},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keystr").Error)
			if tc.composite {
				is.NoErr(db.Exec("CREATE TABLE dest_keystr (a INT, id VARCHAR(20), PRIMARY KEY (a, id))").Error)
				is.NoErr(db.Exec("INSERT INTO dest_keystr VALUES (1,'1000'),(1,'1e3'),(1,'01000'),(1,'1000.0'),(1,' 1000')").Error)
			} else {
				is.NoErr(db.Exec("CREATE TABLE dest_keystr (id VARCHAR(20) PRIMARY KEY)").Error)
				is.NoErr(db.Exec("INSERT INTO dest_keystr VALUES ('1000'),('1e3'),('01000'),('1000.0'),(' 1000')").Error)
			}

			written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keystr", tc.key)})
			is.NoErr(err)
			is.Equal(written, 1)

			var left []string
			is.NoErr(db.Raw("SELECT id FROM dest_keystr").Scan(&left).Error)
			is.Equal(len(left), 4) // exactly one row deleted
			for _, id := range left {
				is.True(id != tc.deleted)
			}
		})
	}

	t.Run("bool on a string column", func(t *testing.T) {
		is := is.New(t)
		is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keystr").Error)
		is.NoErr(db.Exec("CREATE TABLE dest_keystr (id VARCHAR(20) PRIMARY KEY)").Error)
		_, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keystr", opencdc.StructuredData{"id": true})})
		is.True(errors.Is(err, ErrKeyValueTypeMismatch))
	})
}

// The key column types used to be cached for the life of the destination, so
// after ALTER TABLE ... BIGINT a key 1.5 was still sent as for a VARCHAR and
// matched nothing (an acked no-op). Types are read for every batch.
func TestDestination_Delete_KeyTypeChangedAfterOpen(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keyalter").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_keyalter (id VARCHAR(20) PRIMARY KEY)").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_keyalter") })
	is.NoErr(db.Exec("INSERT INTO dest_keyalter VALUES ('1'), ('2')").Error)

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keyalter", opencdc.StructuredData{"id": "1"})})
	is.NoErr(err)
	is.Equal(written, 1)

	is.NoErr(db.Exec("ALTER TABLE dest_keyalter MODIFY id BIGINT").Error)
	_, err = dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keyalter", opencdc.RawData(`{"id":1.5}`))})
	is.True(errors.Is(err, ErrKeyValueTypeMismatch))
}

// An integer key outside the column's range matches nothing: TINYINT 300,
// UNSIGNED -1 and 1e300 were acked no-ops. They are refused; the edges of the
// range still delete their row.
func TestDestination_Delete_IntegerKeyOutOfRange(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_keyrange") })

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	for name, tc := range map[string]struct {
		colType string
		row     string
		key     opencdc.Data
		ok      bool
	}{
		"tinyint 300":          {"TINYINT", "127", opencdc.RawData(`{"id":300}`), false},
		"tinyint max":          {"TINYINT", "127", opencdc.RawData(`{"id":127}`), true},
		"tinyint unsigned -1":  {"TINYINT UNSIGNED", "0", opencdc.StructuredData{"id": int64(-1)}, false},
		"tinyint unsigned 255": {"TINYINT UNSIGNED", "255", opencdc.StructuredData{"id": 255}, true},
		"int 2^31":             {"INT", "1", opencdc.RawData(`{"id":2147483648}`), false},
		"bigint 1e300":         {"BIGINT", "1", opencdc.RawData(`{"id":1e300}`), false},
		"bigint unsigned max":  {"BIGINT UNSIGNED", "18446744073709551615", opencdc.RawData(`{"id":18446744073709551615}`), true},
		"bigint unsigned 2^64": {"BIGINT UNSIGNED", "1", opencdc.RawData(`{"id":18446744073709551616}`), false},
		"bigint min":           {"BIGINT", "-9223372036854775808", opencdc.RawData(`{"id":-9223372036854775808}`), true},
		"bigint huge exponent": {"BIGINT", "1", opencdc.RawData(`{"id":1e1000000}`), false},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keyrange").Error)
			is.NoErr(db.Exec("CREATE TABLE dest_keyrange (id " + tc.colType + " PRIMARY KEY)").Error)
			is.NoErr(db.Exec("INSERT INTO dest_keyrange VALUES (" + tc.row + ")").Error)

			written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keyrange", tc.key)})
			var count int64
			is.NoErr(db.Raw("SELECT COUNT(*) FROM dest_keyrange").Scan(&count).Error)
			if tc.ok {
				is.NoErr(err)
				is.Equal(written, 1)
				is.Equal(count, int64(0))
			} else {
				is.True(errors.Is(err, ErrKeyValueTypeMismatch))
				is.Equal(written, 0)
				is.Equal(count, int64(1))
			}
		})
	}
}

func keysTestUpsert(table string, after opencdc.StructuredData) opencdc.Record {
	metadata := opencdc.Metadata{}
	metadata.SetCollection(table)
	return opencdc.Record{Operation: opencdc.OperationCreate, Metadata: metadata, Payload: opencdc.Change{After: after}}
}

// Values of unique-index columns decide which row ON DUPLICATE KEY UPDATE
// overwrites, and MySQL converts on assignment: {"id":1.5} into a BIGINT key
// became 2 and overwrote row 2. They get the same checks as delete keys.
func TestDestination_Upsert_KeyValuesAreTyped(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_upkey") })

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	for name, tc := range map[string]struct {
		ddl, row string
		after    opencdc.StructuredData
		code     error
	}{
		"fraction into bigint primary key": {
			"(id BIGINT PRIMARY KEY, v VARCHAR(10))", "(2, 'orig')",
			opencdc.StructuredData{"id": 1.5, "v": "new"},
			ErrKeyValueTypeMismatch,
		},
		"fraction into unique column": {
			"(id INT AUTO_INCREMENT PRIMARY KEY, ext BIGINT UNIQUE, v VARCHAR(10))", "(1, 2, 'orig')",
			opencdc.StructuredData{"ext": 1.5, "v": "new"},
			ErrKeyValueTypeMismatch,
		},
		"list into primary key": {
			"(id BIGINT PRIMARY KEY, v VARCHAR(10))", "(2, 'orig')",
			opencdc.StructuredData{"id": []any{2}, "v": "new"},
			ErrKeyValueNotScalar,
		},
		"float 1e15 into varchar primary key": {
			"(id VARCHAR(30) PRIMARY KEY, v VARCHAR(10))", "('1e15', 'orig')",
			opencdc.StructuredData{"id": float64(1e15), "v": "new"},
			ErrKeyValueTypeMismatch,
		},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_upkey").Error)
			is.NoErr(db.Exec("CREATE TABLE dest_upkey " + tc.ddl).Error)
			is.NoErr(db.Exec("INSERT INTO dest_upkey VALUES " + tc.row).Error)

			written, err := dest.Write(ctx, []opencdc.Record{keysTestUpsert("dest_upkey", tc.after)})
			is.True(errors.Is(err, tc.code))
			is.Equal(written, 0)

			var v []string
			is.NoErr(db.Raw("SELECT v FROM dest_upkey").Scan(&v).Error)
			is.Equal(v, []string{"orig"}) // nothing overwritten, nothing added
		})
	}

	t.Run("number into varchar primary key is stored as its text", func(t *testing.T) {
		is := is.New(t)
		is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_upkey").Error)
		is.NoErr(db.Exec("CREATE TABLE dest_upkey (id VARCHAR(30) PRIMARY KEY, v VARCHAR(10))").Error)
		written, err := dest.Write(ctx, []opencdc.Record{
			keysTestUpsert("dest_upkey", opencdc.StructuredData{"id": float64(1000), "v": "a"}),
			keysTestUpsert("dest_upkey", opencdc.StructuredData{"id": int64(1000), "v": "b"}),
		})
		is.NoErr(err)
		is.Equal(written, 2)
		var rows []struct{ ID, V string }
		is.NoErr(db.Raw("SELECT id, v FROM dest_upkey").Scan(&rows).Error)
		is.Equal(len(rows), 1)
		is.Equal(rows[0].ID, "1000")
		is.Equal(rows[0].V, "b")
	})
}

// Floats on string key columns are only sent when MySQL's text for them is the
// plain integer (whole and below 1e15); 1e21 used to be sent as
// "1000000000000000000000" while MySQL stores '1e21', an acked no-op.
func TestDestination_Delete_FloatOnStringKey(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keyfloat").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_keyfloat (id VARCHAR(30) PRIMARY KEY)").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_keyfloat") })
	is.NoErr(db.Exec("INSERT INTO dest_keyfloat VALUES ('1e21'), ('1e15'), ('1.5'), ('100')").Error)

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	for _, f := range []float64{1e21, 1e15, 1.5} {
		_, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keyfloat", opencdc.StructuredData{"id": f})})
		is.True(errors.Is(err, ErrKeyValueTypeMismatch))
	}
	written, err := dest.Write(ctx, []opencdc.Record{keysTestDelete("dest_keyfloat", opencdc.StructuredData{"id": float64(100)})})
	is.NoErr(err)
	is.Equal(written, 1)
	var count int64
	is.NoErr(db.Raw("SELECT COUNT(*) FROM dest_keyfloat").Scan(&count).Error)
	is.Equal(count, int64(3))
}

// An ENUM key compares a number with the member's index and a BIT key rounds
// a fraction, so both used to be acked no-ops. ENUM and SET take member names
// only; BIT takes whole numbers within its width.
func TestDestination_Delete_EnumAndBitKeys(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS dest_keyenum")
		db.Exec("DROP TABLE IF EXISTS dest_keybit")
	})
	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keyenum").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_keyenum (id ENUM('a','b','c') PRIMARY KEY)").Error)
	is.NoErr(db.Exec("INSERT INTO dest_keyenum VALUES ('a'), ('b')").Error)
	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_keybit").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_keybit (id BIT(8) PRIMARY KEY)").Error)
	is.NoErr(db.Exec("INSERT INTO dest_keybit VALUES (b'00000001'), (b'00000101')").Error)

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	for name, tc := range map[string]struct {
		table string
		key   opencdc.Data
	}{
		"enum structured number": {"dest_keyenum", opencdc.StructuredData{"id": 1}},
		"enum raw number":        {"dest_keyenum", opencdc.RawData(`{"id":1}`)},
		"bit fraction":           {"dest_keybit", opencdc.RawData(`{"id":1.5}`)},
		"bit over width":         {"dest_keybit", opencdc.StructuredData{"id": 256}},
		"bit negative":           {"dest_keybit", opencdc.StructuredData{"id": -1}},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			_, err := dest.Write(ctx, []opencdc.Record{keysTestDelete(tc.table, tc.key)})
			is.True(errors.Is(err, ErrKeyValueTypeMismatch))
		})
	}

	written, err := dest.Write(ctx, []opencdc.Record{
		keysTestDelete("dest_keyenum", opencdc.StructuredData{"id": "a"}),
		keysTestDelete("dest_keybit", opencdc.RawData(`{"id":5}`)),
	})
	is.NoErr(err)
	is.Equal(written, 2)
	var n int64
	is.NoErr(db.Raw("SELECT COUNT(*) FROM dest_keyenum").Scan(&n).Error)
	is.Equal(n, int64(1))
	is.NoErr(db.Raw("SELECT COUNT(*) FROM dest_keybit").Scan(&n).Error)
	is.Equal(n, int64(1))
}

// A delete for a table that does not exist (dropped, or a name in the wrong
// case where table names are case-sensitive) said the table had no primary
// key. It has its own code now.
func TestDestination_Delete_TableNotFound(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_nosuchtable").Error)
	is.NoErr(db.Exec("DROP TABLE IF EXISTS DestMixedCase").Error)
	is.NoErr(db.Exec("CREATE TABLE DestMixedCase (id BIGINT PRIMARY KEY)").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS DestMixedCase") })
	var lctn int
	is.NoErr(db.Raw("SELECT @@lower_case_table_names").Scan(&lctn).Error)

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	tables := []string{"dest_nosuchtable", "otherdb_that_does_not_exist.t"}
	if lctn == 0 {
		tables = append(tables, "destmixedcase") // case-sensitive names
	}
	for _, table := range tables {
		_, err := dest.Write(ctx, []opencdc.Record{keysTestDelete(table, opencdc.StructuredData{"id": 1})})
		is.True(errors.Is(err, ErrTableNotFound))
	}
}
