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

// Regression tests for #204 and the float64 position bug: the last-read key was
// stored in the position as the driver returned it and read back lossily
// (float64, base64 text), so a resume skipped or repeated rows. Each case
// snapshots six rows with fetchSize=2, acks the first k records for every
// k in 1..5, "crashes", restarts from the last acked position and requires
// exactly the remaining rows, in order, once. Invariants 2 and 3.

package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestSnapshotResume_KeyTypes(t *testing.T) {
	tests := []struct {
		name   string
		sqlTyp string
		// literals are SQL literals in ascending key order.
		literals []string
	}{
		{"bigint_above_2pow53", "BIGINT", []string{"-9007199254740995", "1", "9007199254740993", "9007199254740996", "9007199254740997", "9223372036854775807"}},
		// Values above 2^63-1 halt the source (CodeUnsignedOverflow, see
		// unsigned_overflow_integration_test.go), so they cannot be resumed
		// from. This covers the unsigned path up to the largest value it
		// emits; the full range needs the lossless mapping of #86.
		{"bigint_unsigned", "BIGINT UNSIGNED", []string{"1", "9007199254740993", "9007199254740995", "9223372036854775805", "9223372036854775806", "9223372036854775807"}},
		{"int_negative", "INT", []string{"-2147483648", "-5", "-1", "0", "7", "2147483647"}},
		{"decimal_negative", "DECIMAL(10,2)", []string{"-300.00", "-200.00", "-100.00", "-50.00", "-1.00", "5.00"}},
		{"varchar", "VARCHAR(20)", []string{"'a'", "'b'", "'c-1'", "'d'", "'e'", "'z'"}},
		{"varbinary", "VARBINARY(8)", []string{"X'01'", "X'0102'", "X'02'", "X'80'", "X'FF00'", "X'FFFF'"}},
		{"datetime", "DATETIME(6)", []string{"'2020-01-01 00:00:00.000001'", "'2020-01-01 00:00:00.000002'", "'2020-06-01 12:00:00'", "'2021-01-01 00:00:00'", "'2021-01-01 00:00:01'", "'2030-12-31 23:59:59.999999'"}},
		{"date", "DATE", []string{"'2020-01-01'", "'2020-01-02'", "'2020-02-01'", "'2021-01-01'", "'2022-01-01'", "'2030-12-31'"}},
		{"double", "DOUBLE", []string{"-1.5", "-0.1", "0.1", "0.2", "1.5", "1e10"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			db := testutils.NewDB(t)

			table := "kt_" + tc.name
			is.NoErr(db.Exec("DROP TABLE IF EXISTS " + table).Error)
			is.NoErr(db.Exec(fmt.Sprintf("CREATE TABLE %s (k %s NOT NULL PRIMARY KEY, v INT NOT NULL)", table, tc.sqlTyp)).Error)
			for i, lit := range tc.literals {
				is.NoErr(db.Exec(fmt.Sprintf("INSERT INTO %s (k, v) VALUES (%s, %d)", table, lit, i+1)).Error)
			}
			n := len(tc.literals)
			cfg := func() map[string]string {
				return map[string]string{"tables": "^" + table + "$", "snapshot.fetchSize": "2"}
			}

			for k := 1; k < n; k++ {
				t.Run(fmt.Sprintf("resume_after_%d", k), func(t *testing.T) {
					is := is.New(t)

					runCtx, cancel := context.WithCancel(ctx)
					source := testSourceOpen(runCtx, is, cfg(), nil)
					var pos opencdc.Position
					for i := 1; i <= k; i++ {
						recs := readOne(ctx, is, source)
						is.Equal(payloadInt(recs[0]), int64(i))
						is.NoErr(source.Ack(ctx, recs[0].Position))
						pos = recs[0].Position
					}
					killSource(cancel, source)

					restartCtx, restartCancel := context.WithCancel(ctx)
					defer restartCancel()
					source2 := testSourceOpen(restartCtx, is, cfg(), pos)
					t.Cleanup(func() { teardownForCleanup(source2) })

					for want := k + 1; want <= n; want++ {
						recs := readOne(ctx, is, source2)
						is.Equal(recs[0].Operation, opencdc.OperationSnapshot)
						is.Equal(payloadInt(recs[0]), int64(want)) // no row skipped or repeated
						is.NoErr(source2.Ack(ctx, recs[0].Position))
					}

					// Nothing further: the snapshot is exhausted and CDC is idle.
					idleCtx, idleCancel := context.WithTimeout(ctx, time.Second)
					defer idleCancel()
					extra, err := source2.ReadN(idleCtx, 1)
					is.True(len(extra) == 0)
					_ = err
				})
			}
		})
	}
}

func readOne(ctx context.Context, is *is.I, source *Source) []opencdc.Record {
	is.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	recs, err := source.ReadN(readCtx, 1)
	is.NoErr(err)
	is.Equal(len(recs), 1)
	return recs
}

func payloadInt(rec opencdc.Record) int64 {
	switch v := rec.Payload.After.(opencdc.StructuredData)["v"].(type) {
	case int32:
		return int64(v)
	case int64:
		return v
	default:
		return -1
	}
}

// Upgrade test: a snapshot position written by the previous build (version 1,
// bare JSON numbers) must resume exactly, including a BIGINT key above 2^53
// that the previous build wrote exactly but read back through float64.
func TestSnapshotResume_FromPreviousBuildPosition(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	const table = "kt_legacy_pos"
	is.NoErr(db.Exec("DROP TABLE IF EXISTS " + table).Error)
	is.NoErr(db.Exec("CREATE TABLE " + table + " (k BIGINT NOT NULL PRIMARY KEY, v INT NOT NULL)").Error)
	// 9007199254740995 is not representable in float64 and rounds up to ...996,
	// which is a row: a float64 round trip skips it.
	keys := []string{"9007199254740993", "9007199254740995", "9007199254740996", "9007199254740999"}
	for i, k := range keys {
		is.NoErr(db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (%s, %d)", table, k, i+1)).Error)
	}
	cfg := func() map[string]string {
		return map[string]string{"tables": "^" + table + "$", "snapshot.fetchSize": "2"}
	}

	// Run on this build to learn the P0 and shape of a real position...
	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg(), nil)
	var pos opencdc.Position
	for range 2 {
		recs := readOne(ctx, is, source)
		is.NoErr(source.Ack(ctx, recs[0].Position))
		pos = recs[0].Position
	}
	killSource(cancel, source)

	// ...then rewrite it exactly as the previous build would have: version 1 and
	// bare numbers.
	legacy := fmt.Sprintf(`{"version":1,"snapshot_position":{"snapshots":{%q:{"SingleKey":{"last_read":%s,"snapshot_end":%s}}},"cdc_start":%s}}`,
		table, keys[1], keys[3], cdcStartJSON(is, pos))

	restartCtx, restartCancel := context.WithCancel(ctx)
	defer restartCancel()
	source2 := testSourceOpen(restartCtx, is, cfg(), opencdc.Position(legacy))
	t.Cleanup(func() { teardownForCleanup(source2) })

	for want := 3; want <= 4; want++ {
		recs := readOne(ctx, is, source2)
		is.Equal(payloadInt(recs[0]), int64(want))
		is.NoErr(source2.Ack(ctx, recs[0].Position))
	}
}

func cdcStartJSON(is *is.I, pos opencdc.Position) string {
	is.Helper()
	cdcStart := parseTestPosition(is, pos).SnapshotPosition.CDCStart
	is.True(cdcStart != nil)
	raw, err := json.Marshal(cdcStart)
	is.NoErr(err)
	return string(raw)
}

// Positions written by this build record the key column's type, which is the
// only way a resume can tell a VARCHAR key "10" from a DECIMAL key "10" after
// the column has been retyped.
func TestSnapshotPosition_RecordsKeyColumnType(t *testing.T) {
	tests := []struct {
		sqlTyp  string
		literal string
		want    string
	}{
		{"VARCHAR(20)", "'a'", "varchar"},
		{"DECIMAL(10,2)", "1.50", "decimal"},
		{"BIGINT UNSIGNED", "9223372036854775807", "bigint"},
		{"VARBINARY(8)", "X'01'", "varbinary"},
		{"DATETIME(6)", "'2020-01-01 00:00:00'", "datetime"},
		{"INT", "7", "int"},
	}
	for i, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			db := testutils.NewDB(t)

			table := fmt.Sprintf("kt_type_%d", i)
			is.NoErr(db.Exec("DROP TABLE IF EXISTS " + table).Error)
			is.NoErr(db.Exec(fmt.Sprintf("CREATE TABLE %s (k %s NOT NULL PRIMARY KEY, v INT NOT NULL)", table, tc.sqlTyp)).Error)
			is.NoErr(db.Exec(fmt.Sprintf("INSERT INTO %s (k, v) VALUES (%s, 1)", table, tc.literal)).Error)

			runCtx, cancel := context.WithCancel(ctx)
			source := testSourceOpen(runCtx, is, map[string]string{"tables": "^" + table + "$"}, nil)
			t.Cleanup(func() { killSource(cancel, source) })

			recs := readOne(ctx, is, source)
			pos := parseTestPosition(is, recs[0].Position).SnapshotPosition.Snapshots[table]
			is.True(pos.SingleKey != nil)
			is.Equal(pos.SingleKey.KeyType, tc.want)
		})
	}
}
