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
	set := func(s ...string) *[]string { c := append([]string{}, s...); return &c }
	cdc := func(snapshotted *[]string) startPlan {
		return startPlan{
			mode:        startModeCDCResume,
			cdcPosition: &common.CdcPosition{SnapshottedTables: snapshotted},
		}
	}
	derefOrNil := func(p *[]string) any {
		if p == nil {
			return nil
		}
		return *p
	}

	tests := []struct {
		name         string
		plan         startPlan
		snapshot     bool
		tables       []string
		explicit     bool
		wantCode     bool
		wantWarnings int
		wantCarry    any
	}{
		{"fresh, snapshot on: records current tables", startPlan{mode: startModeFresh}, true, []string{"b", "a"}, true, false, 0, []string{"a", "b"}},
		{"fresh, snapshot off: records the empty set", startPlan{mode: startModeFresh}, false, []string{"a"}, true, false, 0, []string{}},
		{"mid-snapshot resume, snapshot on", startPlan{mode: startModeSnapshotResume}, true, []string{"a"}, true, false, 0, []string{"a"}},
		{"cdc, same tables", cdc(set("a", "b")), true, []string{"a", "b"}, true, false, 0, []string{"a", "b"}},
		{"cdc, table removed", cdc(set("a", "b")), true, []string{"a"}, true, false, 0, []string{"a", "b"}},
		{"cdc, explicit list gained a table: error", cdc(set("a")), true, []string{"a", "b"}, true, true, 0, nil},
		{"cdc, pattern gained a table: warn, set unchanged", cdc(set("a")), true, []string{"a", "b"}, false, false, 1, []string{"a"}},
		{"cdc, snapshot flipped on after cdc-only run: error", cdc(set()), true, []string{"a"}, true, true, 0, nil},
		{"cdc, snapshot flipped on, pattern: warn", cdc(set()), true, []string{"a"}, false, false, 1, []string{}},
		{"cdc legacy position, snapshot on: warn and start recording", cdc(nil), true, []string{"b", "a"}, true, false, 1, []string{"a", "b"}},
		{"cdc legacy position, snapshot off: stays unknown", cdc(nil), false, []string{"a"}, true, false, 0, nil},
		{"cdc known position, snapshot off: set kept", cdc(set("a")), false, []string{"a", "b"}, true, false, 0, []string{"a"}},
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
				is.True(strings.Contains(err.Error(), "[b]") || strings.Contains(err.Error(), "[a]"))
				is.True(coded.Fix != "")
				return
			}
			is.NoErr(err)
			is.Equal(len(got.warnings), tc.wantWarnings)
			is.Equal(derefOrNil(got.carry), tc.wantCarry)
		})
	}
}
