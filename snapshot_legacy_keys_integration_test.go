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

// Review blocker on #206: a snapshot position written by an older build stores a
// text, DECIMAL or binary key as base64 text. Reading that as the key itself
// sought from the wrong value and silently skipped rows (a VARCHAR key rows a..z
// resumed with 0 of 22 rows). The fix resolves the key against the column type,
// or restarts the table from the first row. Invariant 3.

package mysql

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// legacySinglePosition builds a snapshot position exactly as the previous build
// wrote it: version 1, a bare JSON value per key. lastRead and end are raw JSON.
func legacySinglePosition(ctx context.Context, is *is.I, table, lastRead, end, keyType string, cfg map[string]string) opencdc.Position {
	is.Helper()
	// A real run supplies a valid cdc_start (P0) for the position.
	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, nil)
	recs := readOne(ctx, is, source)
	is.NoErr(source.Ack(ctx, recs[0].Position))
	cdcStart := cdcStartJSON(is, recs[0].Position)
	killSource(cancel, source)

	keyTypeField := ""
	if keyType != "" {
		keyTypeField = fmt.Sprintf(`,"key_type":%q`, keyType)
	}

	return opencdc.Position(fmt.Sprintf(
		`{"version":1,"snapshot_position":{"snapshots":{%q:{"SingleKey":{"last_read":%s,"snapshot_end":%s%s}}},"cdc_start":%s}}`,
		table, lastRead, end, keyTypeField, cdcStart))
}

type savedKeyCase struct {
	name     string
	sqlTyp   string
	literals []string // ascending
	lastRead string   // raw JSON for the saved key
	end      string
	wantFrom int    // first v expected after resume (1-based)
	keyType  string // recorded key_type in the saved position, if any
}

// resumeFromSavedKey creates a table, builds a position holding the given raw
// key (as an older build wrote it, or as a column of another type left it), and
// requires the resume to deliver exactly rows wantFrom..N, in order.
func resumeFromSavedKey(t *testing.T, tc savedKeyCase) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	table := "klg_" + tc.name
	is.NoErr(db.Exec("DROP TABLE IF EXISTS " + table).Error)
	is.NoErr(db.Exec(fmt.Sprintf("CREATE TABLE %s (k %s NOT NULL PRIMARY KEY, v INT NOT NULL)", table, tc.sqlTyp)).Error)
	for i, lit := range tc.literals {
		is.NoErr(db.Exec(fmt.Sprintf("INSERT INTO %s (k, v) VALUES (%s, %d)", table, lit, i+1)).Error)
	}
	cfg := func() map[string]string {
		return map[string]string{"tables": "^" + table + "$", "snapshot.fetchSize": "10"}
	}

	pos := legacySinglePosition(ctx, is, table, tc.lastRead, tc.end, tc.keyType, cfg())

	restartCtx, restartCancel := context.WithCancel(ctx)
	defer restartCancel()
	source := testSourceOpen(restartCtx, is, cfg(), pos)
	t.Cleanup(func() { teardownForCleanup(source) })

	for want := tc.wantFrom; want <= len(tc.literals); want++ {
		recs := readOne(ctx, is, source)
		is.Equal(payloadInt(recs[0]), int64(want)) // no row skipped
		is.NoErr(source.Ack(ctx, recs[0].Position))
	}
}

func TestSnapshotResume_FromPreviousBuildPosition_ByKeyType(t *testing.T) {
	letters := make([]string, 0, 26)
	for c := 'a'; c <= 'z'; c++ {
		letters = append(letters, fmt.Sprintf("'%c'", c))
	}

	tests := []savedKeyCase{
		{"varchar", "VARCHAR(20)", letters, `"` + b64("d") + `"`, `"` + b64("z") + `"`, 5, ""},
		{"decimal", "DECIMAL(10,2)", []string{"-300.00", "-200.00", "-100.00", "-50.00", "-1.00", "5.00"}, `"` + b64("-200.00") + `"`, `"` + b64("5.00") + `"`, 3, ""},
		{"varbinary", "VARBINARY(8)", []string{"X'01'", "X'0102'", "X'02'", "X'80'", "X'FF00'", "X'FFFF'"}, `"` + base64.StdEncoding.EncodeToString([]byte{0x01, 0x02}) + `"`, `"//8="`, 3, ""},
		{"date", "DATE", []string{"'2020-01-01'", "'2020-01-02'", "'2020-02-01'", "'2021-01-01'", "'2022-01-01'", "'2030-12-31'"}, `"2020-01-02T00:00:00Z"`, `"2030-12-31T00:00:00Z"`, 3, ""},
		{"int_numbers_unchanged", "BIGINT", []string{"1", "2", "3", "4", "5", "6"}, `3`, `6`, 4, ""},
		// Cannot be interpreted: a text value for an INT column. The table must
		// restart from its first row, never skip.
		{"uninterpretable_restarts_table", "INT", []string{"1", "2", "3", "4", "5", "6"}, `"ZA=="`, `"eg=="`, 1, ""},
		// Not valid base64 for a text column: same fallback.
		{"invalid_base64_restarts_table", "VARCHAR(20)", []string{"'a'", "'b'", "'c'", "'d'", "'e'", "'f'"}, `"not base64!"`, `"eg=="`, 1, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { resumeFromSavedKey(t, tc) })
	}
}

// Review round 3 on #206: a saved key whose kind does not fit the key column's
// current type (the column was retyped) skipped rows through MySQL's implicit
// cross-class conversion: last_read 3 on a VARCHAR column holding a..f resumed
// with none of them. The table must restart from its first row instead. This
// also failed on main.
func TestSnapshotResume_RetypedKeyColumn_RestartsTable(t *testing.T) {
	six := []string{"1", "2", "3", "4", "5", "6"}
	letters := []string{"'a'", "'b'", "'c'", "'d'", "'e'", "'f'"}
	spread := []string{"1", "2", "9", "10", "11", "100"}

	tests := []savedKeyCase{
		// INT -> VARCHAR, position from an older build (bare number).
		{"int_to_varchar_legacy", "VARCHAR(20)", letters, `3`, `6`, 1, ""},
		// INT -> VARCHAR, position from this build (tagged int64).
		{"int_to_varchar_tagged", "VARCHAR(20)", letters, `{"t":"int64","v":"3"}`, `{"t":"int64","v":"6"}`, 1, ""},
		// DATETIME -> INT: a tagged time saved for a column that is now numeric.
		{"datetime_to_int_tagged", "INT", six, `{"t":"time","v":"2030-01-01T00:00:00Z"}`, `{"t":"time","v":"2031-01-01T00:00:00Z"}`, 1, ""},
		// VARBINARY -> DATE: tagged bytes saved for a column that is now temporal.
		{"bytes_to_date_tagged", "DATE", []string{"'2020-01-01'", "'2020-01-02'", "'2020-02-01'", "'2021-01-01'", "'2022-01-01'", "'2030-12-31'"}, `{"t":"bytes","v":"Ag=="}`, `{"t":"bytes","v":"Bg=="}`, 1, ""},
		// Round 4 blocker: the bytes "10" (base64 MTA=) saved from a VARCHAR or
		// DECIMAL key, on an INT or BIGINT column holding 1,2,9,10,11,100, resumed
		// at 11 and skipped 2 and 9.
		{"bytes10_on_int", "INT", spread, `{"t":"bytes","v":"MTA="}`, `{"t":"bytes","v":"MTAw"}`, 1, ""},
		{"bytes10_on_bigint", "BIGINT", spread, `{"t":"bytes","v":"MTA="}`, `{"t":"bytes","v":"MTAw"}`, 1, ""},
		{"bytes10_on_int_from_varchar", "INT", spread, `{"t":"bytes","v":"MTA="}`, `{"t":"bytes","v":"MTAw"}`, 1, "varchar"},
		// VARCHAR <-> DECIMAL: both are bytes, but "10" sorts before "9" as text and
		// after it as a number, so a key saved under one cannot seek in the other.
		{"varchar_to_decimal", "DECIMAL(10,0)", spread, `{"t":"bytes","v":"MTA="}`, `{"t":"bytes","v":"MTAw"}`, 1, "varchar"},
		{"decimal_to_varchar", "VARCHAR(20)", []string{"'1'", "'10'", "'100'", "'11'", "'2'", "'9'"}, `{"t":"bytes","v":"Mg=="}`, `{"t":"bytes","v":"OQ=="}`, 1, "decimal"},
		{"varchar_to_varbinary", "VARBINARY(20)", []string{"'a'", "'b'", "'c'", "'d'", "'e'", "'f'"}, `{"t":"bytes","v":"Yw=="}`, `{"t":"bytes","v":"Zg=="}`, 1, "varchar"},
		// Controls: the same class as the column still resumes where it stopped.
		{"decimal_unchanged_recorded", "DECIMAL(10,0)", spread, `{"t":"bytes","v":"MTA="}`, `{"t":"bytes","v":"MTAw"}`, 5, "decimal"},
		{"varchar_unchanged_recorded", "VARCHAR(20)", []string{"'1'", "'10'", "'100'", "'11'", "'2'", "'9'"}, `{"t":"bytes","v":"MTE="}`, `{"t":"bytes","v":"OQ=="}`, 5, "varchar"},
		{"int_unchanged_tagged", "INT", six, `{"t":"int64","v":"3"}`, `{"t":"int64","v":"6"}`, 4, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { resumeFromSavedKey(t, tc) })
	}
}
