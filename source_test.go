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
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestSource_Teardown(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

// Regression test for issue #158: which persisted positions skip the snapshot.
// The inputs are literal JSON, as persisted by earlier connector builds, so a
// format drift on either side shows up here. Invariant 3.
func TestResolveStartPlan(t *testing.T) {
	tests := []struct {
		name         string
		position     string
		wantMode     startMode
		wantSnapshot bool // snapshotPosition carried into the snapshot iterator
		wantCDC      string
	}{
		{
			name:     "legacy cdc position, no version envelope",
			position: `{"cdc_position":{"name":"binlog.000003","pos":1234}}`,
			wantMode: startModeCDCResume,
			wantCDC:  "binlog.000003:1234",
		},
		{
			name:     "legacy cdc position with row index and prev",
			position: `{"cdc_position":{"name":"binlog.000003","pos":1234,"idx":2,"prev":{"name":"binlog.000003","pos":900}}}`,
			wantMode: startModeCDCResume,
			wantCDC:  "binlog.000003:1234",
		},
		{
			name:     "versioned cdc position",
			position: `{"version":1,"cdc_position":{"name":"binlog.000007","pos":4}}`,
			wantMode: startModeCDCResume,
			wantCDC:  "binlog.000007:4",
		},
		{
			name:         "legacy snapshot position without cdc_start resumes the snapshot",
			position:     `{"snapshot_position":{"snapshots":{"users":{"SingleKey":{"last_read":5,"snapshot_end":10}}}}}`,
			wantMode:     startModeFresh,
			wantSnapshot: true,
		},
		{
			name:         "snapshot position with cdc_start resumes snapshot and seeds cdc from P0",
			position:     `{"version":1,"snapshot_position":{"snapshots":{"users":{"SingleKey":{"last_read":5,"snapshot_end":10}}},"cdc_start":{"name":"binlog.000002","pos":777}}}`,
			wantMode:     startModeSnapshotResume,
			wantSnapshot: true,
			wantCDC:      "binlog.000002:777",
		},
		{
			name:     "empty object is a fresh start",
			position: `{}`,
			wantMode: startModeFresh,
		},
		{
			name:         "cdc position wins over a stale snapshot position",
			position:     `{"snapshot_position":{"snapshots":{}},"cdc_position":{"name":"binlog.000009","pos":42}}`,
			wantMode:     startModeCDCResume,
			wantSnapshot: false,
			wantCDC:      "binlog.000009:42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			pos, err := common.ParseSDKPosition(opencdc.Position(tc.position))
			is.NoErr(err)

			plan := resolveStartPlan(pos)
			is.Equal(plan.mode, tc.wantMode)
			is.Equal(plan.snapshotPosition != nil, tc.wantSnapshot)

			if tc.wantCDC == "" {
				is.True(plan.cdcPosition == nil)
				return
			}
			is.True(plan.cdcPosition != nil)
			is.Equal(fmt.Sprintf("%s:%d", plan.cdcPosition.Name, plan.cdcPosition.Pos), tc.wantCDC)
		})
	}
}

func TestTablesAreExplicit(t *testing.T) {
	tests := []struct {
		rules []string
		want  bool
	}{
		{[]string{"users"}, true},
		{[]string{"users", " orders "}, true},
		{[]string{"+users"}, true},
		{[]string{"*"}, false},
		{[]string{"users", "*"}, false},
		{[]string{"user.*"}, false},
		{[]string{"-users"}, false},
		{[]string{"^users$"}, false},
		{nil, false},
	}
	for _, tc := range tests {
		is := is.New(t)
		is.Equal(tablesAreExplicit(tc.rules), tc.want)
	}
}

// Silent loss of pre-existing rows for tables added after CDC began (review of
// #195). Invariant 3.
func TestResolveCoverage(t *testing.T) {
	cov := common.NewTableCoverage
	cdc := func(c *common.TableCoverage) startPlan {
		return startPlan{mode: startModeCDCResume, cdcPosition: &common.CdcPosition{TableCoverage: c}}
	}
	many := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("t%04d", i)
		}
		return out
	}

	tests := []struct {
		name         string
		plan         startPlan
		snapshot     bool
		tables       []string
		explicit     bool
		wantCode     bool
		wantWarnings int
		want         *common.TableCoverage // nil: carry nil (legacy stays unknown)
	}{
		{"fresh, snapshot on: records current tables", startPlan{mode: startModeFresh}, true, []string{"b", "a"}, true, false, 0, cov([]string{"a", "b"}, nil, nil)},
		{"fresh, snapshot off: records known-empty", startPlan{mode: startModeFresh}, false, []string{"a"}, true, false, 0, cov(nil, nil, nil)},
		{"mid-snapshot resume, snapshot on", startPlan{mode: startModeSnapshotResume}, true, []string{"a"}, true, false, 0, cov([]string{"a"}, nil, nil)},
		{"cdc, same tables", cdc(cov([]string{"a", "b"}, nil, nil)), true, []string{"a", "b"}, true, false, 0, cov([]string{"a", "b"}, nil, nil)},
		{"cdc, table removed: forgotten", cdc(cov([]string{"a", "b"}, nil, nil)), true, []string{"a"}, true, false, 0, cov([]string{"a"}, nil, nil)},
		{"cdc, table removed then added back, explicit: error", cdc(cov([]string{"a"}, nil, nil)), true, []string{"a", "b"}, true, true, 0, nil},
		{"cdc, explicit list gained a table: error", cdc(cov([]string{"a"}, nil, nil)), true, []string{"a", "b"}, true, true, 0, nil},
		{"cdc, pattern gained a table: warn once and record cdc-only", cdc(cov([]string{"a"}, nil, nil)), true, []string{"a", "b"}, false, false, 1, cov([]string{"a"}, nil, []string{"b"})},
		{"cdc, same pattern tables on the next restart: silent", cdc(cov([]string{"a"}, nil, []string{"b"})), true, []string{"a", "b"}, false, false, 0, cov([]string{"a"}, nil, []string{"b"})},
		{"cdc, snapshot flipped on after cdc-only run: error", cdc(cov(nil, nil, nil)), true, []string{"a"}, true, true, 0, nil},
		{"cdc, snapshot flipped on, pattern: warn once", cdc(cov(nil, nil, nil)), true, []string{"a"}, false, false, 1, cov(nil, nil, []string{"a"})},
		{"cdc legacy, snapshot on: adopt as UNVERIFIED, warn", cdc(nil), true, []string{"b", "a"}, true, false, 1, cov(nil, []string{"a", "b"}, nil)},
		{"cdc adopted-unverified restart: keeps warning, never promoted", cdc(cov(nil, []string{"a"}, nil)), true, []string{"a"}, true, false, 1, cov(nil, []string{"a"}, nil)},
		{"cdc adopted-unverified, new explicit table: error", cdc(cov(nil, []string{"a"}, nil)), true, []string{"a", "b"}, true, true, 0, nil},
		{"cdc legacy, snapshot off: stays unknown", cdc(nil), false, []string{"a"}, true, false, 0, nil},
		{"cdc known, snapshot off: restricted, nothing checked", cdc(cov([]string{"a", "b"}, nil, nil)), false, []string{"a", "c"}, true, false, 0, cov([]string{"a"}, nil, nil)},
		{"hash form, same set", cdc(cov(many(300), nil, nil)), true, many(300), true, false, 0, cov(many(300), nil, nil)},
		{"hash form, set changed, explicit: error", cdc(cov(many(300), nil, nil)), true, many(301), true, true, 0, nil},
		{"hash form, set changed, pattern: warn and re-record", cdc(cov(many(300), nil, nil)), true, many(301), false, false, 1, cov(many(301), nil, nil)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			got, err := resolveCoverage(tc.plan, tc.snapshot, tc.tables, tc.explicit)
			if tc.wantCode {
				is.True(errors.Is(err, ErrTablesAddedAfterCDC))
				var coded *Error
				is.True(errors.As(err, &coded))
				is.Equal(coded.Code, "mysql.snapshot.tables_added_after_cdc")
				is.Equal(coded.ConfigPath, "tables")
				is.True(coded.Fix != "")
				is.True(strings.Contains(coded.Fix, "^name$")) // the real escape for unanchored rules
				return
			}
			is.NoErr(err)
			is.Equal(len(got.warnings), tc.wantWarnings)
			if tc.want == nil {
				is.True(got.carry == nil)
				return
			}
			is.True(got.carry != nil)
			// The set content, not the form: hashed coverage compares by hash.
			is.Equal(*got.carry, *tc.want)
		})
	}

	t.Run("hashed explicit list: no direction claimed, no impossible fix", func(t *testing.T) {
		is := is.New(t)
		_, err := resolveCoverage(cdc(cov(many(300), nil, nil)), true, many(299), true) // a table was REMOVED
		var coded *Error
		is.True(errors.As(err, &coded))
		is.True(strings.Contains(coded.Message, "configured table set changed"))
		is.True(!strings.Contains(coded.Message, "were added after"))
		is.True(strings.Contains(coded.Fix, "reset the pipeline's position"))
		is.True(strings.Contains(coded.Fix, "restore the `tables` list"))
		is.True(!strings.Contains(coded.Fix, "remove them from"))
	})

	t.Run("hashed pattern mode: warning claims no direction", func(t *testing.T) {
		is := is.New(t)
		got, err := resolveCoverage(cdc(cov(many(300), nil, nil)), true, many(299), false)
		is.NoErr(err)
		is.Equal(len(got.warnings), 1)
		is.True(strings.Contains(got.warnings[0], "configured table set changed"))
		is.True(!strings.Contains(got.warnings[0], "were added after"))
	})

	t.Run("error names the added tables", func(t *testing.T) {
		is := is.New(t)
		_, err := resolveCoverage(cdc(cov([]string{"a"}, nil, nil)), true, []string{"a", "late"}, true)
		is.True(strings.Contains(err.Error(), "late"))
	})
}

// snapshot.unsafe was removed. true must fail validation, even for a pipeline
// that is already in CDC (validation cannot see the position), with a stable,
// actionable error; false is accepted so existing configs keep loading.
func TestSourceConfig_UnsafeSnapshotRemoved(t *testing.T) {
	validate := func(cfg map[string]string) error {
		cfg["dsn"] = "root:pw@tcp(127.0.0.1:3306)/db"
		cfg["tables"] = "*"
		source := &Source{}
		return sdk.Util.ParseConfig(context.Background(), cfg, source.Config(), Connector.NewSpecification().SourceParams)
	}

	t.Run("true is rejected with code, path and fix", func(t *testing.T) {
		is := is.New(t)
		err := validate(map[string]string{"snapshot.unsafe": "true"})
		is.True(errors.Is(err, ErrUnsafeSnapshotRemoved))
		var coded *Error
		is.True(errors.As(err, &coded))
		is.Equal(coded.Code, "mysql.config.unsafe_snapshot_removed")
		is.Equal(coded.ConfigPath, "snapshot.unsafe")
		is.True(strings.Contains(coded.Fix, "tableConfig.<table>.sortingColumn"))
		is.True(strings.Contains(coded.Fix, "already in CDC"))
	})

	t.Run("false is ignored", func(t *testing.T) {
		is := is.New(t)
		is.NoErr(validate(map[string]string{"snapshot.unsafe": "false"}))
	})

	t.Run("absent is fine", func(t *testing.T) {
		is := is.New(t)
		is.NoErr(validate(map[string]string{}))
	})
}

func TestNewSnapshotIterator_RejectsTableWithoutKey(t *testing.T) {
	is := is.New(t)
	_, err := newSnapshotIterator(snapshotIteratorConfig{
		database:         "db",
		tablePrimaryKeys: common.TableKeys{"ok": {"id"}, "keyless": {}},
	})
	is.True(errors.Is(err, ErrTableWithoutKey))
}
