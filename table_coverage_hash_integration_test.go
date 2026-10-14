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
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/matryer/is"
)

// Above common.MaxListedTables the table coverage in a CDC position is a hash.
// A MySQL table name may contain a newline, and the hash used to join names with
// one, so the tables hc_x and hc_y and the one table "hc_x\nhc_y" had the same
// hash. A restart that swapped the second set in was treated as unchanged, so
// the new table's existing rows were never snapshotted and nothing said so.
// Invariant 3. This runs against a real server because the premise is that the
// server really accepts such a name and returns it byte for byte.
func TestRestart_HashedCoverage_NewlineNamedTable_IsDetected(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	const newlineName = "hc_x\nhc_y"

	quote := func(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }
	create := func(name string) {
		is.Helper()
		is.NoErr(db.Exec("DROP TABLE IF EXISTS " + quote(name)).Error)
		is.NoErr(db.Exec("CREATE TABLE " + quote(name) + " (id BIGINT PRIMARY KEY, val VARCHAR(20))").Error)
	}
	drop := func(names ...string) {
		for _, n := range names {
			_ = db.Exec("DROP TABLE IF EXISTS " + quote(n)).Error
		}
	}

	fillers := make([]string, common.MaxListedTables)
	for i := range fillers {
		fillers[i] = fmt.Sprintf("hc_f%03d", i)
	}
	all := append(append([]string{}, fillers...), "hc_x", "hc_y", newlineName)
	drop(all...)
	t.Cleanup(func() { drop(all...) })

	for _, n := range fillers {
		create(n)
	}
	create("hc_x")
	create("hc_y")

	// The server must keep the name exactly, or the premise does not hold.
	create(newlineName)
	var stored []string
	is.NoErr(db.Raw("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE 'hc\\_x%'").Scan(&stored).Error)
	is.True(containsString(stored, newlineName))
	drop(newlineName)

	// Run 1: hc_x and hc_y configured, 202 tables, so the coverage is a hash.
	first := append(append([]string{}, fillers...), "hc_x", "hc_y")
	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, map[string]string{"tables": strings.Join(first, ",")}, nil)
	is.NoErr(db.Exec("INSERT INTO hc_f000 (id, val) VALUES (1, 'cdc')").Error)
	recs := drainRecords(ctx, is, source)
	is.True(len(recs) > 0)
	pos := recs[len(recs)-1].Position
	killSource(cancel, source)
	cov := coverageOf(is, pos)
	is.True(cov.Hashed())
	is.Equal(cov.Count, len(first))

	// hc_x and hc_y are replaced by one table that already holds rows.
	drop("hc_x", "hc_y")
	create(newlineName)
	is.NoErr(db.Exec("INSERT INTO " + quote(newlineName) + " (id, val) VALUES (1, 'existing')").Error)

	// Run 2: an explicit list that now names the new table. Its existing rows
	// were never snapshotted, so Open must refuse, as it does for an unhashed
	// coverage.
	second := append(append([]string{}, fillers...), newlineName)
	_, err := openSource(ctx, is, map[string]string{"tables": strings.Join(second, ",")}, pos)
	is.True(errors.Is(err, ErrTablesAddedAfterCDC))
}

func containsString(in []string, s string) bool {
	for _, v := range in {
		if v == s {
			return true
		}
	}
	return false
}
