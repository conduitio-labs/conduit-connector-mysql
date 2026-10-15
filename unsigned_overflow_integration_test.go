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
	"strings"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

const (
	maxInt64Text  = "9223372036854775807"
	maxUint64Text = "18446744073709551615"
	// foldedMaxUint64 is what the snapshot path used to emit for
	// 18446744073709551615: its ASCII digits folded as a big-endian integer.
	foldedMaxUint64 = int64(3474867090068222261)
)

// readUntilError reads from the source until it returns an error or the
// deadline passes, acking what it reads so the snapshot can hand over to CDC.
func readUntilError(ctx context.Context, t *testing.T, is *is.I, source *Source) ([]opencdc.Record, error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var recs []opencdc.Record
	for time.Now().Before(deadline) {
		readCtx, cancel := context.WithTimeout(ctx, time.Second)
		got, err := source.ReadN(readCtx, 10)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return recs, err
		}
		for _, r := range got {
			is.NoErr(source.Ack(ctx, r.Position))
		}
		recs = append(recs, got...)
	}
	t.Fatal("the source did not halt within 30s")
	return nil, nil
}

func requireOverflowHalt(t *testing.T, is *is.I, err error, recs []opencdc.Record, table string) {
	t.Helper()
	is.True(errors.Is(err, ErrUnsignedOverflow)) // the source must halt with the stable error
	var coded *Error
	is.True(errors.As(err, &coded))
	is.Equal(coded.Code, CodeUnsignedOverflow)
	is.True(strings.Contains(coded.Message, table+".u"))
	is.True(strings.Contains(coded.Message, maxUint64Text))

	for _, r := range recs {
		after, ok := r.Payload.After.(opencdc.StructuredData)
		is.True(ok)
		is.True(after["u"] != foldedMaxUint64) // the corrupted value must never be emitted
		is.True(after["id"] != int64(2))       // nor any record for the overflowing row
	}
}

// Regression test for ConduitIO/conduit-connector-mysql#86 (snapshot path): a
// BIGINT UNSIGNED above 2^63-1 was emitted as a different, valid int64
// (18446744073709551615 as 3474867090068222261). It must halt instead.
func TestUnsignedOverflow_SnapshotHalts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	const table = "u64_overflow_snapshot"
	is.NoErr(db.Exec("DROP TABLE IF EXISTS " + table).Error)
	is.NoErr(db.Exec("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, u BIGINT UNSIGNED NOT NULL)").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })
	is.NoErr(db.Exec("INSERT INTO " + table + " VALUES (1, " + maxInt64Text + "), (2, " + maxUint64Text + ")").Error)

	source := testSourceOpen(ctx, is, map[string]string{"tables": table}, nil)
	t.Cleanup(func() { teardownForCleanup(source) })

	recs, err := readUntilError(ctx, t, is, source)
	requireOverflowHalt(t, is, err, recs, table)

	// The largest value that fits is still emitted exactly.
	is.Equal(len(recs), 1)
	is.Equal(recs[0].Payload.After.(opencdc.StructuredData)["u"], int64(9223372036854775807))
}

// Regression test for ConduitIO/conduit-connector-mysql#86 (CDC path): canal
// hands BIGINT UNSIGNED over as uint64, which was passed on unconverted above
// 2^63-1 and only failed later in the Avro encoder, with no stable error.
func TestUnsignedOverflow_CDCHalts(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	const table = "u64_overflow_cdc"
	is.NoErr(db.Exec("DROP TABLE IF EXISTS " + table).Error)
	is.NoErr(db.Exec("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, u BIGINT UNSIGNED NOT NULL)").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })

	source := testSourceOpen(ctx, is, map[string]string{"tables": table, "snapshot.enabled": "false"}, nil)
	t.Cleanup(func() { teardownForCleanup(source) })

	is.NoErr(db.Exec("INSERT INTO " + table + " VALUES (1, " + maxInt64Text + ")").Error)
	is.NoErr(db.Exec("INSERT INTO " + table + " VALUES (2, " + maxUint64Text + ")").Error)

	recs, err := readUntilError(ctx, t, is, source)
	requireOverflowHalt(t, is, err, recs, table)

	is.Equal(len(recs), 1)
	is.Equal(recs[0].Payload.After.(opencdc.StructuredData)["u"], int64(9223372036854775807))
}
