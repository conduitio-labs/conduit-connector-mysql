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
)

// TestPassedTarget_AcrossSixDigitRotation is a regression test for #238.
// While a resume looks for the saved event, a position synced in a later
// binlog file means the saved event will not come, and everything from there
// on is new. With binlog names compared as text, the rotation from
// mysql-bin.999999 to mysql-bin.1000000 read as moving backwards: the target
// stayed set, and onRows dropped every later rows event as "before the saved
// event" (invariant 3).
func TestPassedTarget_AcrossSixDigitRotation(t *testing.T) {
	tests := []struct {
		name      string
		target    common.ReplicationEventPosition
		at        common.ReplicationEventPosition
		wantClear bool
	}{
		{"rotation past 999999", binlogPos("mysql-bin.999999", 900), binlogPos("mysql-bin.1000000", 4), true},
		{"rotation past 9999999", binlogPos("mysql-bin.9999999", 900), binlogPos("mysql-bin.10000000", 4), true},
		{"later offset in the same file", binlogPos("mysql-bin.999999", 900), binlogPos("mysql-bin.999999", 1200), true},
		{"earlier offset in the same file", binlogPos("mysql-bin.999999", 900), binlogPos("mysql-bin.999999", 600), false},
		{"earlier file", binlogPos("mysql-bin.1000000", 900), binlogPos("mysql-bin.999999", 4000), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := tt.target
			h := &cdcEventHandler{target: &target}
			if err := h.passedTarget(tt.at); err != nil {
				t.Fatal(err)
			}
			if cleared := h.target == nil; cleared != tt.wantClear {
				t.Fatalf("target %v at %v: cleared = %v, want %v", tt.target, tt.at, cleared, tt.wantClear)
			}
		})
	}
}

func binlogPos(name string, off uint32) common.ReplicationEventPosition {
	return common.ReplicationEventPosition{Name: name, Pos: off}
}
