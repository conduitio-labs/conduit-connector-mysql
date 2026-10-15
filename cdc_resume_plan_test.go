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
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/google/go-cmp/cmp"
)

func TestPlanResume(t *testing.T) {
	ev := func(name string, pos uint32) common.ReplicationEventPosition {
		return common.ReplicationEventPosition{Name: name, Pos: pos}
	}
	ptr := func(p common.ReplicationEventPosition) *common.ReplicationEventPosition { return &p }

	tests := []struct {
		name  string
		start common.CdcPosition
		want  resumePlan
	}{{
		name:  "fresh start delivers everything from the start position",
		start: common.CdcPosition{ReplicationEventPosition: ev("binlog.000003", 900)},
		want:  resumePlan{from: ev("binlog.000003", 900), prev: ev("binlog.000003", 900)},
	}, {
		name: "this version: replay point, skip the event up to and including Index",
		start: common.CdcPosition{
			ReplicationEventPosition: ev("binlog.000003", 900), Index: 6,
			PrevPosition: ptr(ev("binlog.000003", 700)), ReplayFrom: ptr(ev("binlog.000003", 650)),
		},
		want: resumePlan{
			from: ev("binlog.000003", 650), target: ptr(ev("binlog.000003", 900)), skipRows: 7,
			prev: ev("binlog.000003", 700),
		},
	}, {
		name: "older version: read the file from its start, Index is exact",
		start: common.CdcPosition{
			ReplicationEventPosition: ev("binlog.000003", 900), Index: 6,
			PrevPosition: ptr(ev("binlog.000002", 700)),
		},
		want: resumePlan{
			from: ev("binlog.000003", 4), target: ptr(ev("binlog.000003", 900)), skipRows: 7,
			prev: ev("binlog.000003", 4),
		},
	}, {
		name: "older version after a mid-event resume: Index is not exact, deliver the event in full",
		start: common.CdcPosition{
			ReplicationEventPosition: ev("binlog.000003", 900), Index: 2,
			PrevPosition: ptr(ev("binlog.000003", 900)),
		},
		want: resumePlan{
			from: ev("binlog.000003", 4), target: ptr(ev("binlog.000003", 900)), skipRows: 0,
			prev: ev("binlog.000003", 4),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := planResume(tc.start)
			got.note = ""
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(resumePlan{})); diff != "" {
				t.Fatalf("(-want +got):\n%s", diff)
			}
		})
	}
}

func TestReplicationEventPositionCompare(t *testing.T) {
	a := common.ReplicationEventPosition{Name: "binlog.000002", Pos: 900}
	for _, tc := range []struct {
		b    common.ReplicationEventPosition
		want int
	}{
		{common.ReplicationEventPosition{Name: "binlog.000002", Pos: 900}, 0},
		{common.ReplicationEventPosition{Name: "binlog.000002", Pos: 901}, -1},
		{common.ReplicationEventPosition{Name: "binlog.000002", Pos: 4}, 1},
		{common.ReplicationEventPosition{Name: "binlog.000003", Pos: 4}, -1},
		{common.ReplicationEventPosition{Name: "binlog.000001", Pos: 99999}, 1},
	} {
		if got := a.Compare(tc.b); got != tc.want {
			t.Errorf("Compare(%v) = %d, want %d", tc.b, got, tc.want)
		}
	}
}
