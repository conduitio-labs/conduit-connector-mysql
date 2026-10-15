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

// Regression tests for issue #218: the table coverage in a CDC position held
// bare table names. With the DSN pointed at another database that has tables of
// the same names and the position kept, the coverage compared equal, the
// restart skipped the snapshot, and the existing rows of the other database were
// never delivered. Nothing was logged. Invariant 3.

package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

const otherDatabase = "cov218_other"

// otherDSN returns the test DSN pointed at otherDatabase.
func otherDSN(is *is.I) string {
	is.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)
	cfg.DBName = otherDatabase
	return cfg.FormatDSN()
}

func testDatabase(is *is.I) string {
	is.Helper()
	cfg, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)
	return cfg.DBName
}

// openSourceDSN is openSource against an explicit DSN.
func openSourceDSN(ctx context.Context, is *is.I, dsn string, cfg map[string]string, pos opencdc.Position) (*Source, error) {
	source := &Source{}
	cfg["dsn"] = dsn
	cfg["cdc.disableLogs"] = "true"
	is.NoErr(sdk.Util.ParseConfig(ctx, cfg, source.Config(), Connector.NewSpecification().SourceParams))
	return source, source.Open(ctx, pos)
}

// setUpOtherDatabase creates a second database holding a crash_rows table with
// the same definition as the test database's, and rows that already exist
// before the pipeline is pointed at it.
func setUpOtherDatabase(t *testing.T, is *is.I, db testutils.DB) {
	t.Helper()
	is.NoErr(db.Exec("DROP DATABASE IF EXISTS " + otherDatabase).Error)
	is.NoErr(db.Exec("CREATE DATABASE " + otherDatabase).Error)
	t.Cleanup(func() { _ = db.Exec("DROP DATABASE IF EXISTS " + otherDatabase).Error })
	is.NoErr(db.Exec(fmt.Sprintf("CREATE TABLE %s.crash_rows LIKE %s.crash_rows", otherDatabase, testDatabase(is))).Error)
	is.NoErr(db.Exec(fmt.Sprintf("INSERT INTO %s.crash_rows (id, val) VALUES (1, 'existing-1'), (2, 'existing-2'), (3, 'existing-3')", otherDatabase)).Error)
}

// requireDatabaseChanged asserts that Open refused the restart. When it did not
// (the bug), it reports what the restart delivered from the other database's
// three pre-existing rows, which is the data loss.
func requireDatabaseChanged(ctx context.Context, t *testing.T, is *is.I, source *Source, err error) {
	t.Helper()
	if err == nil {
		got := drainRecords(ctx, is, source)
		teardownForCleanup(source)
		t.Fatalf("Open accepted a position recorded for database %q against database %q; "+
			"the restart delivered %d snapshot records of the 3 rows that already existed there (lost)",
			testDatabase(is), otherDatabase, countSnapshots(got))
	}
	is.True(errors.Is(err, ErrDatabaseChanged))
	var coded *Error
	is.True(errors.As(err, &coded))
	is.Equal(coded.Code, CodeDatabaseChanged)
	is.Equal(coded.ConfigPath, "dsn")
	is.True(strings.Contains(coded.Message, otherDatabase))
	is.True(strings.Contains(coded.Message, testDatabase(is)))
	is.True(!strings.Contains(err.Error(), "meroxaadmin")) // never echo the DSN's credentials
}

func TestRestart_DSNPointedAtOtherDatabase_ExplicitList_Fails(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)
	setUpOtherDatabase(t, is, db)

	cfg := func() map[string]string { return map[string]string{"tables": "crash_rows"} }
	pos := cdcPositionAfterOneChange(ctx, is, db, cfg())

	source, err := openSourceDSN(ctx, is, otherDSN(is), cfg(), pos)
	requireDatabaseChanged(ctx, t, is, source, err)
}

func TestRestart_DSNPointedAtOtherDatabase_Pattern_Fails(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)
	setUpOtherDatabase(t, is, db)

	cfg := func() map[string]string { return map[string]string{"tables": "^crash_rows$"} }
	pos := cdcPositionAfterOneChange(ctx, is, db, cfg())

	source, err := openSourceDSN(ctx, is, otherDSN(is), cfg(), pos)
	requireDatabaseChanged(ctx, t, is, source, err)
}

// The escapes the error offers work: the original database resumes, and
// snapshot.enabled=false streams the other database's changes only.
func TestRestart_DSNPointedAtOtherDatabase_Escapes(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)
	setUpOtherDatabase(t, is, db)

	cfg := map[string]string{"tables": "crash_rows"}
	pos := cdcPositionAfterOneChange(ctx, is, db, cfg)

	// Same database: resumes, no snapshot, the database stays recorded.
	runCtx, cancel := context.WithCancel(ctx)
	source, err := openSourceDSN(runCtx, is, testutils.DSN, map[string]string{"tables": "crash_rows"}, pos)
	is.NoErr(err)
	is.NoErr(db.Create(&crashRow{ID: 200, Val: "after"}).Error)
	got := drainRecords(runCtx, is, source)
	is.Equal(countSnapshots(got), 0)
	is.True(hasRecordFor(got, opencdc.OperationCreate, 200))
	is.Equal(coverageOf(is, got[len(got)-1].Position).Database, testDatabase(is))
	killSource(cancel, source)

	// Other database, snapshot explicitly off: the operator opted out of
	// existing rows, so nothing is checked.
	runCtx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	source2, err := openSourceDSN(runCtx2, is, otherDSN(is),
		map[string]string{"tables": "crash_rows", "snapshot.enabled": "false"}, pos)
	is.NoErr(err)
	t.Cleanup(func() { teardownForCleanup(source2) })
}

// Upgrade test: a coverage written by the previous build of this branch has no
// database. It is adopted for the DSN's database with one warning, recorded,
// and from then on a switch to another database is refused.
func TestUpgrade_CoverageWithoutDatabase_IsAdoptedOnce(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	resetTables(is, db)
	is.NoErr(db.Create(&crashRow{ID: 1, Val: "a"}).Error)
	setUpOtherDatabase(t, is, db)

	cfg := func() map[string]string { return map[string]string{"tables": "crash_rows"} }
	pos := cdcPositionAfterOneChange(ctx, is, db, cfg())

	// Rewrite it exactly as the previous build would have written it.
	var raw map[string]any
	is.NoErr(json.Unmarshal(pos, &raw))
	cov := raw["cdc_position"].(map[string]any)["table_coverage"].(map[string]any)
	delete(cov, "database")
	legacy, err := json.Marshal(raw)
	is.NoErr(err)
	is.True(!strings.Contains(string(legacy), `"database"`))
	is.True(strings.Contains(string(legacy), `"snapshotted":["crash_rows"]`))

	is.NoErr(db.Create(&crashRow{ID: 101, Val: "after-upgrade"}).Error)

	logCtx, logs := capturedLogContext()
	runCtx, cancel := context.WithCancel(logCtx)
	source, err := openSourceDSN(runCtx, is, testutils.DSN, cfg(), legacy)
	is.NoErr(err)
	is.True(strings.Contains(logs.String(), `"level":"warn"`))
	is.True(strings.Contains(logs.String(), CodeDatabaseAssumed))
	got := drainRecords(runCtx, is, source)
	is.Equal(countSnapshots(got), 0)
	is.True(hasRecordFor(got, opencdc.OperationCreate, 101))
	upgraded := got[len(got)-1].Position
	c := coverageOf(is, upgraded)
	is.Equal(c.Database, testDatabase(is))
	is.Equal(c.Snapshotted, []string{"crash_rows"})
	killSource(cancel, source)

	// The next start is silent.
	logCtx2, logs2 := capturedLogContext()
	runCtx2, cancel2 := context.WithCancel(logCtx2)
	source2, err := openSourceDSN(runCtx2, is, testutils.DSN, cfg(), upgraded)
	is.NoErr(err)
	is.True(!strings.Contains(logs2.String(), CodeDatabaseAssumed))
	killSource(cancel2, source2)

	// And the recorded database now protects the pipeline.
	source3, err := openSourceDSN(ctx, is, otherDSN(is), cfg(), upgraded)
	requireDatabaseChanged(ctx, t, is, source3, err)
}
