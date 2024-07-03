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
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

var testTables testutils.TestTables

func TestSnapshotIterator_EmptyTable(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.TestConnection(is)

	testTables.Drop(is, db)
	testTables.Create(is, db)

	it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		db:       db,
		database: "meroxadb",
		tables:   testTables.Tables(),
	})
	is.NoErr(err)
	defer func() { is.NoErr(it.Teardown(ctx)) }()

	_, err = it.Next(ctx)
	if errors.Is(err, ErrSnapshotIteratorDone) {
		return
	}
	is.NoErr(err)
}

func TestSnapshotIterator_MultipleTables(t *testing.T) {
	ctx := testutils.TestContext(t)

	is := is.New(t)

	db := testutils.TestConnection(is)

	testTables.Drop(is, db)
	testTables.Create(is, db)
	testTables.InsertData(is, db)

	it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		db:       db,
		database: "meroxadb",
		tables:   testTables.Tables(),
	})
	is.NoErr(err)
	defer func() { is.NoErr(it.Teardown(ctx)) }()

	var recs []sdk.Record

	for {
		rec, err := it.Next(ctx)
		if errors.Is(err, ErrSnapshotIteratorDone) {
			break
		}
		is.NoErr(err)

		recs = append(recs, rec)

		err = it.Ack(ctx, rec.Position)
		is.NoErr(err)
	}

	is.Equal(len(recs), 100) // received a different amount of records
	for _, rec := range recs {
		is.Equal(rec.Operation, sdk.OperationSnapshot)
	}
}

func TestSnapshotIterator_SmallFetchSize(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.TestConnection(is)

	testTables.Drop(is, db)
	testTables.Create(is, db)
	testTables.InsertData(is, db)

	it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		db:       db,
		database: "meroxadb",
		tables:   testTables.Tables(),
	})
	is.NoErr(err)
	defer func() { is.NoErr(it.Teardown(ctx)) }()

	var recs []sdk.Record

	for {
		rec, err := it.Next(ctx)
		if errors.Is(err, ErrSnapshotIteratorDone) {
			break
		}
		is.NoErr(err)

		recs = append(recs, rec)

		err = it.Ack(ctx, rec.Position)
		is.NoErr(err)
	}

	is.Equal(len(recs), 100) // received a different amount of records
	for _, rec := range recs {
		is.Equal(rec.Operation, sdk.OperationSnapshot)
	}
}

func TestSnapshotIterator_RestartOnPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.TestConnection(is)

	testTables.Drop(is, db)
	testTables.Create(is, db)
	testTables.InsertData(is, db)

	// Read the first 10 records

	var breakPosition position
	{
		it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
			db:       db,
			database: "meroxadb",
			tables:   testTables.Tables(),
		})
		is.NoErr(err)
		defer func() { is.NoErr(it.Teardown(ctx)) }()

		var recs []sdk.Record
		for i := 0; i < 10; i++ {
			rec, err := it.Next(ctx)
			if errors.Is(err, ErrSnapshotIteratorDone) {
				break
			}
			is.NoErr(err)

			recs = append(recs, rec)

			err = it.Ack(ctx, rec.Position)
			is.NoErr(err)
		}

		breakPosition, err = parseSDKPosition(recs[len(recs)-1].Position)
		is.NoErr(err)
	}

	// read the remaining 90 records

	it, err := newSnapshotIterator(ctx, snapshotIteratorConfig{
		db:            db,
		startPosition: breakPosition,
		database:      "meroxadb",
		tables:        testTables.Tables(),
	})
	is.NoErr(err)

	var recs []sdk.Record
	for {
		rec, err := it.Next(ctx)
		if errors.Is(err, ErrSnapshotIteratorDone) {
			break
		}
		is.NoErr(err)

		recs = append(recs, rec)

		err = it.Ack(ctx, rec.Position)
		is.NoErr(err)
	}

	is.Equal(len(recs), 90)
	for _, rec := range recs {
		is.Equal(rec.Operation, sdk.OperationSnapshot)
	}
}
