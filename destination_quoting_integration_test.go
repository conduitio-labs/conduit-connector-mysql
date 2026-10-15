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

func quotingTestRecord(table string, op opencdc.Operation, key, after opencdc.StructuredData) opencdc.Record {
	metadata := opencdc.Metadata{}
	metadata.SetCollection(table)
	return opencdc.Record{Operation: op, Metadata: metadata, Key: key, Payload: opencdc.Change{After: after}}
}

// Table and column names come from records, so they are quoted as identifiers:
// reserved words and backticks work, and a field name cannot change the SQL.
func TestDestination_Write_QuotesIdentifiers(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS `select`").Error)
	is.NoErr(db.Exec("CREATE TABLE `select` (`key` BIGINT PRIMARY KEY, `order` VARCHAR(10), `we``ird` VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS `select`") })

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	written, err := dest.Write(ctx, []opencdc.Record{
		quotingTestRecord("select", opencdc.OperationCreate, opencdc.StructuredData{"key": 1},
			opencdc.StructuredData{"key": 1, "order": "a", "we`ird": "b"}),
		quotingTestRecord("select", opencdc.OperationCreate, opencdc.StructuredData{"key": 2},
			opencdc.StructuredData{"key": 2, "order": "c", "we`ird": "d"}),
	})
	is.NoErr(err)
	is.Equal(written, 2)

	var got struct {
		Order string
		Weird string
	}
	is.NoErr(db.Raw("SELECT `order` AS `order`, `we``ird` AS weird FROM `select` WHERE `key` = 1").Scan(&got).Error)
	is.Equal(got.Order, "a")
	is.Equal(got.Weird, "b")

	written, err = dest.Write(ctx, []opencdc.Record{
		quotingTestRecord("select", opencdc.OperationDelete, opencdc.StructuredData{"key": 1}, nil),
	})
	is.NoErr(err)
	is.Equal(written, 1)

	var count int64
	is.NoErr(db.Raw("SELECT COUNT(*) FROM `select`").Scan(&count).Error)
	is.Equal(count, int64(1))
}

// A field name written to look like SQL is a column name that does not exist,
// not a statement.
func TestDestination_Write_FieldNameCannotInjectSQL(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_inject").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_inject (id BIGINT PRIMARY KEY, v VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_inject") })
	is.NoErr(db.Exec("INSERT INTO dest_inject VALUES (1, 'keep')").Error)

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	// Unquoted, this field name closes the column list, supplies its own
	// VALUES and ON DUPLICATE KEY UPDATE, and comments out the rest, so the
	// write would overwrite v of row 1.
	evil := "v) VALUES (?, ?) ON DUPLICATE KEY UPDATE v='pwned' -- "
	written, err := dest.Write(ctx, []opencdc.Record{
		quotingTestRecord("dest_inject", opencdc.OperationCreate, opencdc.StructuredData{"id": 1},
			opencdc.StructuredData{"id": 1, evil: "y"}),
	})
	is.True(errors.Is(err, ErrUnknownColumn))
	is.Equal(written, 0)

	var v string
	is.NoErr(db.Raw("SELECT v FROM dest_inject WHERE id = 1").Scan(&v).Error)
	is.Equal(v, "keep")
}

// A qualified collection, db.table, still writes to and deletes from the table
// in that database.
func TestDestination_Write_QualifiedTable(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("CREATE DATABASE IF NOT EXISTS dest_other").Error)
	t.Cleanup(func() { db.Exec("DROP DATABASE IF EXISTS dest_other") })
	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_other.t").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_other.t (id BIGINT PRIMARY KEY, v VARCHAR(10))").Error)

	dest, cleanup := testDestination(ctx, is)
	defer cleanup()

	written, err := dest.Write(ctx, []opencdc.Record{
		quotingTestRecord("dest_other.t", opencdc.OperationCreate, opencdc.StructuredData{"id": 1}, opencdc.StructuredData{"id": 1, "v": "a"}),
		quotingTestRecord("dest_other.t", opencdc.OperationCreate, opencdc.StructuredData{"id": 2}, opencdc.StructuredData{"id": 2, "v": "b"}),
		quotingTestRecord("dest_other.t", opencdc.OperationDelete, opencdc.StructuredData{"id": 1}, nil),
	})
	is.NoErr(err)
	is.Equal(written, 3)

	var ids []int64
	is.NoErr(db.Raw("SELECT id FROM dest_other.t ORDER BY id").Scan(&ids).Error)
	is.Equal(ids, []int64{2})
}
