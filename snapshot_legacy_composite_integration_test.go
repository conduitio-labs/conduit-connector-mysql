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

// Review blocker on #206 for composite keys: a composite position written by an
// older build holds base64 text for its VARCHAR columns and no key names. Every
// column must be resolved against its own type, and the resume must be exactly
// lexicographic (#189). Lives here, not with the single-key tests, because it
// needs both fixes. Invariant 3.

package mysql

import (
	"context"
	"fmt"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestSnapshotResume_FromPreviousBuildPosition_CompositeTextKey(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	const table = "klg_composite"
	is.NoErr(db.Exec("DROP TABLE IF EXISTS " + table).Error)
	is.NoErr(db.Exec("CREATE TABLE " + table + " (a VARCHAR(20) NOT NULL, b INT NOT NULL, c VARCHAR(20) NOT NULL, v INT NOT NULL, PRIMARY KEY (a, b, c))").Error)

	type row struct {
		a string
		b int
		c string
	}
	var rows []row
	for _, a := range []string{"x", "y"} {
		for b := 1; b <= 2; b++ {
			for _, c := range []string{"p", "q"} {
				rows = append(rows, row{a, b, c})
			}
		}
	}
	for i, r := range rows {
		is.NoErr(db.Exec(fmt.Sprintf("INSERT INTO %s (a, b, c, v) VALUES ('%s', %d, '%s', %d)", table, r.a, r.b, r.c, i+1)).Error)
	}

	cfg := func() map[string]string {
		return map[string]string{"tables": "^" + table + "$", "snapshot.fetchSize": "3"}
	}

	// A real run supplies a valid cdc_start (P0).
	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg(), nil)
	first := readOne(ctx, is, source)
	is.NoErr(source.Ack(ctx, first[0].Position))
	cdcStart := cdcStartJSON(is, first[0].Position)
	killSource(cancel, source)

	last := rows[len(rows)-1]
	for k := 1; k < len(rows); k++ {
		t.Run(fmt.Sprintf("resume_after_%d", k), func(t *testing.T) {
			is := is.New(t)
			at := rows[k-1]

			// Exactly as the previous build wrote it: no key names, text as
			// base64, numbers as numbers.
			legacy := fmt.Sprintf(
				`{"version":1,"snapshot_position":{"snapshots":{%q:{"MultipleKey":[`+
					`{"key_name":"","last_read":%q,"snapshot_end":%q},`+
					`{"key_name":"","last_read":%d,"snapshot_end":%d},`+
					`{"key_name":"","last_read":%q,"snapshot_end":%q}]}},"cdc_start":%s}}`,
				table, b64(at.a), b64(last.a), at.b, last.b, b64(at.c), b64(last.c), cdcStart)

			restartCtx, restartCancel := context.WithCancel(ctx)
			defer restartCancel()
			source := testSourceOpen(restartCtx, is, cfg(), opencdc.Position(legacy))
			t.Cleanup(func() { teardownForCleanup(source) })

			for want := k + 1; want <= len(rows); want++ {
				recs := readOne(ctx, is, source)
				is.Equal(payloadInt(recs[0]), int64(want)) // no row skipped or repeated
				is.NoErr(source.Ack(ctx, recs[0].Position))
			}
		})
	}
}
