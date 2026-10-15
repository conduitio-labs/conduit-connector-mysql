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

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/matryer/is"
)

const strictTable = "strict_types"

func createStrictTable(is *is.I, db testutils.DB) {
	is.Helper()
	is.NoErr(db.Exec("DROP TABLE IF EXISTS " + strictTable).Error)
	is.NoErr(db.Exec(`CREATE TABLE ` + strictTable + ` (
		id   BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		n    INT NULL,
		s    VARCHAR(20) NULL,
		dec1 DECIMAL(30,10) NOT NULL,
		dec2 DECIMAL(10,2) NULL,
		u    BIGINT UNSIGNED NOT NULL
	)`).Error)
}

// wantStrictRow is how strict mode with types.bigintUnsigned=string delivers
// the row insertStrictRow writes, in the snapshot and in CDC alike.
func wantStrictRow(id string) opencdc.StructuredData {
	return opencdc.StructuredData{
		"id": id, "n": nil, "s": nil,
		"dec1": "12345678901234567890.1234567891", "dec2": nil,
		"u": "18446744073709551615",
	}
}

func insertStrictRow(is *is.I, db testutils.DB, id string) {
	is.Helper()
	insertStrictRowU(is, db, id, "18446744073709551615")
}

// insertStrictRowU writes a row with the given BIGINT UNSIGNED value; legacy
// mode halts on values above 2^63-1.
func insertStrictRowU(is *is.I, db testutils.DB, id, u string) {
	is.Helper()
	//nolint:dupword // the row has three NULL columns
	is.NoErr(db.Exec("INSERT INTO " + strictTable + " VALUES (" + id +
		", NULL, NULL, 12345678901234567890.1234567891, NULL, " + u + ")").Error)
}

func strictConfig() map[string]string {
	return map[string]string{"tables": strictTable, "types.mode": "strict", "types.bigintUnsigned": "string"}
}

func payloadOf(is *is.I, rec opencdc.Record) opencdc.StructuredData {
	is.Helper()
	p, ok := rec.Payload.After.(opencdc.StructuredData)
	is.True(ok)
	return p
}

// Strict mode delivers NULL as null, DECIMAL as its exact text and the full
// BIGINT UNSIGNED range, with the same values in the snapshot and in CDC, and
// stamps every position.
func TestStrict_SnapshotAndCDCDeliverExactValues(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	createStrictTable(is, db)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + strictTable) })

	insertStrictRow(is, db, "18446744073709551614") // a key above 2^63-1
	source := testSourceOpen(ctx, is, strictConfig(), nil)
	t.Cleanup(func() { teardownForCleanup(source) })

	recs := drainRecords(ctx, is, source)
	is.Equal(len(recs), 1)
	is.Equal(recs[0].Operation, opencdc.OperationSnapshot)
	is.Equal(payloadOf(is, recs[0]), wantStrictRow("18446744073709551614"))

	insertStrictRow(is, db, "1")
	recs = drainRecords(ctx, is, source)
	is.Equal(len(recs), 1)
	is.Equal(recs[0].Operation, opencdc.OperationCreate)
	is.Equal(payloadOf(is, recs[0]), wantStrictRow("1")) // same shape as the snapshot

	pos, err := common.ParseSDKPosition(recs[0].Position)
	is.NoErr(err)
	is.Equal(pos.Version, common.TypesPositionVersion)
	is.Equal(pos.Types.Name(), "strict/string/string")

	// The registered schema keeps nullability, the null default and the
	// column type property.
	subject, err := recs[0].Metadata.GetPayloadSchemaSubject()
	is.NoErr(err)
	version, err := recs[0].Metadata.GetPayloadSchemaVersion()
	is.NoErr(err)
	sch, err := schema.Get(ctx, subject, version)
	is.NoErr(err)
	text := string(sch.Bytes)
	is.True(strings.Contains(text, `["null","int"]`))
	is.True(strings.Contains(text, `"default":null`))
	is.True(strings.Contains(text, `"mysql.type":"decimal(30,10)"`))
}

// A snapshot resumed after a key above 2^63-1 continues exactly (the key is
// bound as uint64, not as text).
func TestStrict_SnapshotResumesAfterLargeUnsignedKey(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	createStrictTable(is, db)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + strictTable) })

	keys := []string{"1", "9223372036854775807", "9223372036854775808", "18446744073709551614", "18446744073709551615"}
	for _, k := range keys {
		insertStrictRow(is, db, k)
	}
	cfg := strictConfig()
	cfg["snapshot.fetchSize"] = "2"

	for resumeAfter := 1; resumeAfter < len(keys); resumeAfter++ {
		runCtx, cancel := context.WithCancel(ctx)
		source := testSourceOpen(runCtx, is, cfg, nil)
		var pos opencdc.Position
		for i := 0; i < resumeAfter; i++ {
			rec := readOne(ctx, is, source)
			pos = rec[0].Position
		}
		killSource(cancel, source)

		source = testSourceOpen(ctx, is, cfg, pos)
		rest := drainRecords(ctx, is, source)
		teardownForCleanup(source)
		var got []string
		for _, r := range rest {
			if r.Operation == opencdc.OperationSnapshot {
				got = append(got, payloadOf(is, r)["id"].(string))
			}
		}
		is.Equal(got, keys[resumeAfter:])
	}
}

// Transition table, end to end: a legacy pipeline in CDC is refused strict
// without the token, switches with it, and positions carry the stamp from the
// first record on; the stamp then refuses legacy without a token.
func TestStrict_SwitchFromLegacyInCDC(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	createStrictTable(is, db)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + strictTable) })

	legacyCfg := map[string]string{"tables": strictTable, "snapshot.enabled": "false"}
	legacyPos := positionAfterChange(ctx, is, legacyCfg, nil, func() { insertStrictRowU(is, db, "1", "5") })
	parsed, err := common.ParseSDKPosition(legacyPos)
	is.NoErr(err)
	is.True(parsed.Types == nil) // legacy positions are unstamped

	strictCfg := strictConfig()
	strictCfg["snapshot.enabled"] = "false"
	_, err = openSource(ctx, is, strictCfg, legacyPos)
	var e *Error
	is.True(errors.As(err, &e))
	is.Equal(e.Code, CodeTypesStampMismatch)

	strictCfg["types.acceptChangeFrom"] = "legacy"
	strictPos := positionAfterChange(ctx, is, strictCfg, legacyPos, func() { insertStrictRow(is, db, "2") })
	parsed, err = common.ParseSDKPosition(strictPos)
	is.NoErr(err)
	is.Equal(parsed.Types.Name(), "strict/string/string")

	// Unset settings follow the stamp (row 7) ...
	source, err := openSource(ctx, is, map[string]string{"tables": strictTable, "snapshot.enabled": "false"}, strictPos)
	is.NoErr(err)
	teardownForCleanup(source)
	// ... and an explicit change back to legacy needs the token (row 10).
	_, err = openSource(ctx, is, map[string]string{"tables": strictTable, "snapshot.enabled": "false", "types.mode": "legacy"}, strictPos)
	is.True(errors.As(err, &e))
	is.Equal(e.Code, CodeTypesStampMismatch)
}

// A legacy pipeline still in its snapshot cannot switch in place.
func TestStrict_NoSwitchDuringSnapshot(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	createStrictTable(is, db)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + strictTable) })
	insertStrictRowU(is, db, "1", "5")
	insertStrictRowU(is, db, "2", "5")

	runCtx, cancel := context.WithCancel(ctx)
	cfg := map[string]string{"tables": strictTable, "snapshot.fetchSize": "1"}
	source := testSourceOpen(runCtx, is, cfg, nil)
	rec := readOne(ctx, is, source)
	killSource(cancel, source)

	strictCfg := strictConfig()
	strictCfg["types.acceptChangeFrom"] = "legacy"
	_, err := openSource(ctx, is, strictCfg, rec[0].Position)
	var e *Error
	is.True(errors.As(err, &e))
	is.Equal(e.Code, CodeTypesSwitchInSnapshot)
}

// Strict mode refuses a server that does not log full row images; legacy mode
// only warns.
func TestStrict_RefusesPartialRowImageServer(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	createStrictTable(is, db)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + strictTable) })

	is.NoErr(db.Exec("SET GLOBAL binlog_row_image = 'MINIMAL'").Error)
	t.Cleanup(func() { db.Exec("SET GLOBAL binlog_row_image = 'FULL'") })

	_, err := openSource(ctx, is, strictConfig(), nil)
	var e *Error
	is.True(errors.As(err, &e))
	is.Equal(e.Code, CodeBinlogRowImage)

	logCtx, logs := capturedLogContext()
	source, err := openSource(logCtx, is, map[string]string{"tables": strictTable}, nil)
	is.NoErr(err)
	teardownForCleanup(source)
	is.True(strings.Contains(logs.String(), CodeBinlogRowImage))
}

// positionAfterChange opens a source at pos, runs write, reads until quiet and
// returns the last record's position, once the source is killed.
func positionAfterChange(ctx context.Context, is *is.I, cfg map[string]string, pos opencdc.Position, write func()) opencdc.Position {
	is.Helper()
	runCtx, cancel := context.WithCancel(ctx)
	source := testSourceOpen(runCtx, is, cfg, pos)
	write()
	recs := drainRecords(ctx, is, source)
	is.True(len(recs) > 0)
	last := recs[len(recs)-1].Position
	killSource(cancel, source)
	return last
}
