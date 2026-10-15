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
	}, {
		name: "this version, inside a compressed transaction: count from its start",
		start: common.CdcPosition{
			ReplicationEventPosition: ev("binlog.000003", 0), Index: 3,
			PrevPosition: ptr(ev("binlog.000003", 0)), ReplayFrom: ptr(ev("binlog.000003", 650)),
			Compressed: &common.CompressedEventRef{Ordinal: 2, Tables: "fp"},
		},
		want: resumePlan{
			from: ev("binlog.000003", 650), target: ptr(ev("binlog.000003", 0)),
			targetCompressed: &common.CompressedEventRef{Ordinal: 2, Tables: "fp"}, skipRows: 4,
			prev: ev("binlog.000003", 0),
		},
	}, {
		name: "compressed, counted under another table set: deliver the transaction again",
		start: common.CdcPosition{
			ReplicationEventPosition: ev("binlog.000003", 0), Index: 3,
			PrevPosition: ptr(ev("binlog.000003", 0)), ReplayFrom: ptr(ev("binlog.000003", 650)),
			Compressed: &common.CompressedEventRef{Ordinal: 2, Tables: "other"},
		},
		want: resumePlan{from: ev("binlog.000003", 650), prev: ev("binlog.000003", 0)},
	}, {
		name: "compressed without a reference: deliver from the replay point",
		start: common.CdcPosition{
			ReplicationEventPosition: ev("binlog.000003", 0), Index: 3,
			PrevPosition: ptr(ev("binlog.000003", 0)), ReplayFrom: ptr(ev("binlog.000003", 650)),
		},
		want: resumePlan{from: ev("binlog.000003", 650), prev: ev("binlog.000003", 0)},
	}, {
		name: "older version inside a compressed transaction: the file again from its start",
		start: common.CdcPosition{
			ReplicationEventPosition: ev("binlog.000003", 0), Index: 3,
			PrevPosition: ptr(ev("binlog.000003", 0)),
		},
		want: resumePlan{from: ev("binlog.000003", 4), prev: ev("binlog.000003", 4)},
	}, {
		name: "replay point without an offset is ignored: resume like an older version",
		start: common.CdcPosition{
			ReplicationEventPosition: ev("binlog.000003", 900), Index: 6,
			PrevPosition: ptr(ev("binlog.000003", 700)), ReplayFrom: ptr(ev("binlog.000003", 0)),
		},
		want: resumePlan{
			from: ev("binlog.000003", 4), target: ptr(ev("binlog.000003", 900)), skipRows: 7,
			prev: ev("binlog.000003", 4),
		},
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := planResume(tc.start, "fp")
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

func TestTablesFingerprint(t *testing.T) {
	a := tablesFingerprint([]string{"b", "a", "a"})
	if a != tablesFingerprint([]string{"a", "b"}) {
		t.Fatal("order or duplicates changed the fingerprint")
	}
	if a == tablesFingerprint([]string{"a"}) || a == tablesFingerprint([]string{"ab"}) {
		t.Fatal("different table sets share a fingerprint")
	}
	// Length prefixes keep a name boundary from being moved.
	if tablesFingerprint([]string{"1:a"}) == tablesFingerprint([]string{"1", "a"}) {
		t.Fatal("ambiguous encoding")
	}
}
