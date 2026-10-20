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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// readRecordsUntilError acks and returns every record delivered until the
// iterator reports an error (or 10s pass without one).
func readRecordsUntilError(ctx context.Context, it common.Iterator) ([]opencdc.Record, error) {
	var out []opencdc.Record
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		readCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		recs, err := it.ReadN(readCtx, 10)
		cancel()
		for _, r := range recs {
			if err := it.Ack(ctx, r.Position); err != nil {
				return out, fmt.Errorf("ack: %w", err)
			}
			out = append(out, r)
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return out, fmt.Errorf("read: %w", err)
		}
	}
	return out, errors.New("no error within 10s")
}

// Regression test (#232, round 4 review): when the cached columns are stale
// (canal skipped ADD COLUMN ... INVISIBLE), the source re-reads the table's
// columns. Here a later DDL that canal can parse (DROP COLUMN email, ADD COLUMN
// e) ran before the connector reached the event, and it keeps the width: the
// re-read columns fit the row image by count, but not by type, and used to be
// applied anyway, mapping every value after the dropped column onto the wrong
// column with no error. The source must halt instead, before delivering that
// row.
func TestCDCIterator_ReReadColumnsOfAnotherSchemaHalt(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	iterator, _ := testCdcIterator(ctx, t, is)
	exec.insertUsers(1, 1)
	exec.insertUsers(11, 11)
	// Reading one record leaves canal blocked on handing over the second,
	// so the DDL below runs before it reaches the rows written after it.
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	recs, err := iterator.ReadN(readCtx, 1)
	cancel()
	is.NoErr(err)
	is.NoErr(iterator.Ack(ctx, recs[0].Position))

	exec("ALTER TABLE users ADD COLUMN d INT NULL INVISIBLE") // canal cannot parse it: stale columns
	exec("INSERT INTO users (id, username, email, created_at, d) VALUES (2, 'two', 'two@example.com', NOW(), 7)")
	exec("ALTER TABLE users DROP COLUMN email, ADD COLUMN e INT NULL") // canal parses it; the width stays

	delivered, err := readRecordsUntilError(ctx, iterator)
	for _, r := range delivered {
		if r.Key.(opencdc.StructuredData)["id"] == int64(2) {
			t.Fatalf("row 2 was delivered with columns of another schema: %v", r.Payload.After)
		}
	}
	var coded *Error
	if !errors.As(err, &coded) {
		t.Fatalf("want a %s halt, got %v", CodeRowColumnsMismatch, err)
	}
	is.Equal(coded.Code, CodeRowColumnsMismatch)
	is.True(strings.Contains(coded.Message, "written under other columns"))
	is.True(strings.Contains(coded.Fix, "restarting does not help"))
	is.True(errors.Is(iterator.Teardown(ctx), ErrRowColumnsMismatch))
}

// A DROP COLUMN plus an ADD COLUMN while the connector is stopped keeps the
// width, so the old width check passed and every later value landed in the
// wrong column. The Table_map event's column types catch it when the types
// differ, and a restart must not deliver the event either.
func TestCDCIterator_DropAndAddWhileStoppedHaltsOnTypes(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	iterator, teardown := testCdcIterator(ctx, t, is)
	exec.insertUsers(1, 1)
	_, pos := readIDs(ctx, is, iterator, 1)
	teardown()

	exec.insertUsers(2, 2)
	exec("ALTER TABLE users DROP COLUMN email, ADD COLUMN e INT NULL")

	for range 2 { // the halt is the same after a restart
		iterator, _ = testCdcIteratorAtPosition(ctx, t, is, pos)
		delivered, err := readRecordsUntilError(ctx, iterator)
		is.Equal(len(delivered), 0)
		var coded *Error
		if !errors.As(err, &coded) {
			t.Fatalf("want a %s halt, got %v", CodeRowColumnsMismatch, err)
		}
		is.Equal(coded.Code, CodeRowColumnsMismatch)
		_ = iterator.Teardown(ctx)
	}
}

// With binlog_row_metadata=FULL the Table_map event has the column names, so
// the same-type case the types cannot see (a column dropped and another of
// the same type added) is caught too.
func TestCDCIterator_DropAndAddWhileStoppedHaltsOnNamesWithFullMetadata(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)
	setBinlogRowMetadata(ctx, t, is, "FULL")

	iterator, teardown := testCdcIterator(ctx, t, is)
	exec.insertUsers(1, 1)
	_, pos := readIDs(ctx, is, iterator, 1)
	teardown()

	exec.insertUsers(2, 2)
	// email (VARCHAR) is replaced by nick (VARCHAR) at the same position.
	exec("ALTER TABLE users DROP COLUMN email, ADD COLUMN nick VARCHAR(255) NULL AFTER username")

	iterator, _ = testCdcIteratorAtPosition(ctx, t, is, pos)
	delivered, err := readRecordsUntilError(ctx, iterator)
	is.Equal(len(delivered), 0)
	var coded *Error
	if !errors.As(err, &coded) {
		t.Fatalf("want a %s halt, got %v", CodeRowColumnsMismatch, err)
	}
	is.Equal(coded.Code, CodeRowColumnsMismatch)
	is.True(strings.Contains(coded.Message, `"email"`) && strings.Contains(coded.Message, `"nick"`))
	_ = iterator.Teardown(ctx)
}
