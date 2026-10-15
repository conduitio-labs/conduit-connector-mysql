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
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func cdcIndex(is *is.I, pos opencdc.Position) int {
	is.Helper()
	p, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	is.True(p.CdcPosition != nil)
	return p.CdcPosition.Index
}

// Regression test: all records built from one binlog event shared one
// metadata map. The SDK middleware writes each record's schema subject and
// version into it, from its batching goroutine, while earlier records of the
// event are already being read by the caller: a data race, found by the SIGKILL
// chaos harness under -race. Every record must own its metadata.
func TestCDCIterator_RecordsOfOneEventDoNotShareMetadata(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)

	iterator, teardown := testCdcIterator(ctx, t, is)
	defer teardown()

	var users []*testutils.User
	for i := 1; i <= 3; i++ {
		users = append(users, testutils.CreateUser(i))
	}
	is.NoErr(db.Create(users).Error) // one INSERT, one rows event

	var recs []opencdc.Record
	for i := range users {
		recs = append(recs, testutils.ReadAndAssertCreate(ctx, is, iterator, *users[i]))
	}

	recs[0].Metadata["test.marker"] = "first"
	_, leaked := recs[1].Metadata["test.marker"]
	is.True(!leaked) // a write to one record's metadata must not show up in another's
	_, leaked = recs[2].Metadata["test.marker"]
	is.True(!leaked)
}

// Regression test: after a resume in the middle of a multi-row event, the rows
// still to be delivered were numbered from 0 instead of from their place in the
// event. Positions went backwards, and a second restart from such a position
// skipped too few rows and delivered acked rows again.
func TestCDCIterator_ResumeMidEventKeepsRowIndex(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)

	iterator, teardown := testCdcIterator(ctx, t, is)

	var users []*testutils.User
	for i := 1; i <= 10; i++ {
		users = append(users, testutils.CreateUser(i))
	}
	is.NoErr(db.Create(users).Error) // one INSERT, one rows event of 10 rows

	var pos opencdc.Position
	for i := 0; i <= 3; i++ {
		rec := testutils.ReadAndAssertCreate(ctx, is, iterator, *users[i])
		is.Equal(cdcIndex(is, rec.Position), i)
		pos = rec.Position
	}
	teardown()

	// First resume, from row 3: rows 4.. keep their index in the event.
	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, pos)
	for i := 4; i <= 6; i++ {
		rec := testutils.ReadAndAssertCreate(ctx, is, iterator, *users[i])
		is.Equal(cdcIndex(is, rec.Position), i) // was i-4: positions went backwards
		pos = rec.Position
	}
	teardown()

	// Second resume, from row 6: the next record is row 7, not a row that was
	// already delivered.
	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, pos)
	defer teardown()
	for i := 7; i < len(users); i++ {
		rec := testutils.ReadAndAssertCreate(ctx, is, iterator, *users[i])
		is.Equal(cdcIndex(is, rec.Position), i)
	}
}

// readIDs reads and acks until n records arrived or the source has been quiet
// for two seconds, and returns the user ids in delivery order.
func readIDs(ctx context.Context, is *is.I, it common.Iterator, n int) ([]int64, opencdc.Position) {
	is.Helper()
	var ids []int64
	var last opencdc.Position
	for len(ids) < n {
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		recs, err := it.ReadN(readCtx, n-len(ids))
		cancel()
		if err != nil {
			break // quiet: nothing more arrived
		}
		for _, r := range recs {
			is.NoErr(it.Ack(ctx, r.Position))
			ids = append(ids, r.Key.(opencdc.StructuredData)["id"].(int64))
			last = r.Position
		}
	}
	return ids, last
}

func idRange(from, to int64) []int64 {
	var out []int64
	for id := from; id <= to; id++ {
		out = append(out, id)
	}
	return out
}

// Regression test (sev-0, invariant 3): records built after a resume in the
// middle of a binlog event carried the end of that event as their
// PrevPosition, which is where the next restart starts reading. A second
// restart from one of them therefore skipped the rest of the event and applied
// the row offset to the next event: rows of both were never delivered. Present
// since #120 (v0.1.4).
func TestCDCIterator_SecondResumeMidEventLosesNothing(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)

	insert := func(from, to int) {
		var users []*testutils.User
		for i := from; i <= to; i++ {
			users = append(users, testutils.CreateUser(i))
		}
		is.NoErr(db.Create(users).Error) // one INSERT, one rows event
	}

	iterator, teardown := testCdcIterator(ctx, t, is)
	insert(1, 10)
	ids, pos := readIDs(ctx, is, iterator, 4)
	is.Equal(ids, idRange(1, 4))
	teardown() // stop after row 4 of the 10-row event

	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, pos)
	ids, pos = readIDs(ctx, is, iterator, 3)
	is.Equal(ids, idRange(5, 7))
	teardown() // stop again, inside the same event, after row 7

	insert(11, 15) // the next event

	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, pos)
	defer teardown()
	ids, _ = readIDs(ctx, is, iterator, 8)
	if diff := cmp.Diff(idRange(8, 15), ids); diff != "" {
		t.Fatalf("second resume delivered the wrong rows (-want +got):\n%s", diff)
	}
}

// A large INSERT is logged as one Table_map event followed by several rows
// events (binlog_row_event_max_size). A resume inside the second or a later
// rows event starts reading after the Table_map event.
func TestCDCIterator_ResumeInsideLaterRowsEventOfOneStatement(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)

	const n = 3000
	insert := func(from, to int) {
		var users []*testutils.User
		for i := from; i <= to; i++ {
			users = append(users, testutils.CreateUser(i))
		}
		is.NoErr(db.Create(users).Error)
	}

	iterator, teardown := testCdcIterator(ctx, t, is)
	insert(1, n)
	ids, pos := readIDs(ctx, is, iterator, 2500)
	is.Equal(len(ids), 2500)
	p, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	t.Logf("stopped at %s:%d#%d prev %+v", p.CdcPosition.Name, p.CdcPosition.Pos, p.CdcPosition.Index, p.CdcPosition.PrevPosition)
	teardown()

	iterator, teardown = testCdcIteratorAtPosition(ctx, t, is, pos)
	defer teardown()
	ids, _ = readIDs(ctx, is, iterator, n-2500)
	if diff := cmp.Diff(idRange(2501, n), ids); diff != "" {
		t.Fatalf("resume delivered the wrong rows (-want +got):\n%s", diff)
	}
}

// Upgrade test: positions written before ReplayFrom existed still resume
// without a gap. A correct one (PrevPosition is the end of the previous rows
// event) resumes exactly; one written right after a mid-event resume by a
// version with that bug (PrevPosition is the event's own end, Index counts
// from the resume point) delivers the whole event again.
func TestCDCIterator_ResumeFromPositionsOfOlderVersions(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)

	insert := func(from, to int) {
		var users []*testutils.User
		for i := from; i <= to; i++ {
			users = append(users, testutils.CreateUser(i))
		}
		is.NoErr(db.Create(users).Error)
	}

	iterator, teardown := testCdcIterator(ctx, t, is)
	insert(100, 102) // an earlier event in the same binlog file
	insert(1, 10)
	ids, _ := readIDs(ctx, is, iterator, 3)
	is.Equal(ids, idRange(100, 102))
	ids, pos := readIDs(ctx, is, iterator, 4)
	is.Equal(ids, idRange(1, 4))
	teardown()
	insert(11, 12) // a later event

	saved, err := common.ParseSDKPosition(pos)
	is.NoErr(err)
	is.True(saved.CdcPosition.ReplayFrom != nil) // this version writes it

	t.Run("correct position of an older version", func(t *testing.T) {
		is := is.New(t)
		old := *saved.CdcPosition
		old.ReplayFrom = nil
		it, td := testCdcIteratorAtPosition(ctx, t, is, old.ToSDKPosition())
		defer td()
		got, _ := readIDs(ctx, is, it, 8)
		if diff := cmp.Diff(idRange(5, 12), got); diff != "" {
			t.Fatalf("(-want +got):\n%s", diff)
		}
	})

	t.Run("position written after a mid-event resume by an older version", func(t *testing.T) {
		is := is.New(t)
		old := *saved.CdcPosition
		old.ReplayFrom = nil
		own := old.ReplicationEventPosition
		old.PrevPosition = &own // the bug's signature
		old.Index = 1           // relative to the resume point, so unreliable
		it, td := testCdcIteratorAtPosition(ctx, t, is, old.ToSDKPosition())
		defer td()
		got, _ := readIDs(ctx, is, it, 12)
		if diff := cmp.Diff(idRange(1, 12), got); diff != "" {
			t.Fatalf("(-want +got):\n%s", diff)
		}
	})
}
