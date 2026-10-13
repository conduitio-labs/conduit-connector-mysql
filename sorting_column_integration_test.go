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
	"errors"
	"strings"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

type sortDupRow struct {
	ID  int    `gorm:"primaryKey;autoIncrement:false"`
	Grp int    `gorm:"not null"`
	V   string `gorm:"size:20"`
}

type sortNullableRow struct {
	ID  int    `gorm:"primaryKey;autoIncrement:false"`
	Grp *int   `gorm:"uniqueIndex"`
	V   string `gorm:"size:20"`
}

type sortUniqueRow struct {
	ID  int    `gorm:"primaryKey;autoIncrement:false"`
	Grp int    `gorm:"not null;uniqueIndex"`
	V   string `gorm:"size:20"`
}

type sortCompositeRow struct {
	A int    `gorm:"primaryKey;autoIncrement:false"`
	B int    `gorm:"primaryKey;autoIncrement:false"`
	V string `gorm:"size:20"`
}

func openWithSortingColumn(t *testing.T, table, column string, extra map[string]string) (*Source, error) {
	t.Helper()
	is := is.New(t)
	ctx := testutils.TestContext(t)

	cfg := map[string]string{
		"dsn":                testutils.DSN,
		"cdc.disableLogs":    "true",
		"tables":             table,
		"snapshot.fetchSize": "2",
		"tableConfig." + table + ".sortingColumn": column,
	}
	for k, v := range extra {
		cfg[k] = v
	}

	source := &Source{}
	is.NoErr(sdk.Util.ParseConfig(ctx, cfg, source.Config(), Connector.NewSpecification().SourceParams))

	return source, source.Open(ctx, nil)
}

// A sorting column that is not unique makes the snapshot skip every row that
// shares a value with the last row of a chunk. With the rows below and a fetch
// size of 2, "c" and "d" were silently never delivered. Invariant 3.
func TestSource_SortingColumn_RejectsNonUnique(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &sortDupRow{})
	is.NoErr(db.Create(&[]sortDupRow{
		{1, 1, "a"}, {2, 2, "b"}, {3, 2, "c"}, {4, 2, "d"}, {5, 3, "e"},
	}).Error)

	_, err := openWithSortingColumn(t, "sort_dup_rows", "grp", nil)
	is.True(errors.Is(err, ErrSortingColumnNotUnique))
	is.True(strings.Contains(err.Error(), "tableConfig.sort_dup_rows.sortingColumn"))
}

// MIN/MAX and "col > ?" ignore NULL, and a unique index allows many NULLs, so a
// nullable column drops those rows even when it is unique.
func TestSource_SortingColumn_RejectsNullable(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &sortNullableRow{})

	_, err := openWithSortingColumn(t, "sort_nullable_rows", "grp", nil)
	is.True(errors.Is(err, ErrSortingColumnNotUnique))
	is.True(strings.Contains(err.Error(), "nullable"))
}

func TestSource_SortingColumn_RejectsPartOfCompositeKey(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &sortCompositeRow{})

	_, err := openWithSortingColumn(t, "sort_composite_rows", "a", nil)
	is.True(errors.Is(err, ErrSortingColumnNotUnique))
}

func TestSource_SortingColumn_RejectsUnknownColumn(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &sortDupRow{})

	_, err := openWithSortingColumn(t, "sort_dup_rows", "nope", nil)
	is.True(errors.Is(err, ErrSortingColumnNotUnique))
}

// The rejection only applies when a snapshot runs; CDC-only pipelines are not
// affected by their sorting column.
func TestSource_SortingColumn_IgnoredWithoutSnapshot(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &sortDupRow{})

	source, err := openWithSortingColumn(t, "sort_dup_rows", "grp", map[string]string{"snapshot.enabled": "false"})
	is.NoErr(err)
	is.NoErr(source.Teardown(testutils.TestContext(t)))
}

// A NOT NULL unique column (here a non-primary-key one) is accepted and every
// row is delivered across chunk boundaries.
func TestSource_SortingColumn_UniqueNotNullDeliversAllRows(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)
	db := testutils.NewDB(t)
	testutils.CreateTables(is, db, &sortUniqueRow{})
	is.NoErr(db.Create(&[]sortUniqueRow{
		{1, 50, "a"}, {2, 10, "b"}, {3, 40, "c"}, {4, 20, "d"}, {5, 30, "e"},
	}).Error)

	source, err := openWithSortingColumn(t, "sort_unique_rows", "grp", nil)
	is.NoErr(err)
	t.Cleanup(func() { teardownForCleanup(source) })

	var got []string
	for _, rec := range drainRecords(ctx, is, source) {
		is.Equal(rec.Operation, opencdc.OperationSnapshot)
		got = append(got, rec.Payload.After.(opencdc.StructuredData)["v"].(string))
	}
	is.Equal(got, []string{"b", "d", "e", "c", "a"}) // ordered by grp
}
