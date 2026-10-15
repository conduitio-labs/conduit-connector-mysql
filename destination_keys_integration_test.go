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
