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

package common

import (
	"fmt"
	"testing"
)

// TestReplicationEventPosition_Compare is the regression test for #238:
// binlog file names compared as text put "mysql-bin.1000000" before
// "mysql-bin.999999". MySQL and MariaDB pad the sequence number to at least
// six digits and keep counting to 2^31-1.
func TestReplicationEventPosition_Compare(t *testing.T) {
	pos := func(name string, off uint32) ReplicationEventPosition {
		return ReplicationEventPosition{Name: name, Pos: off}
	}
	tests := []struct {
		a, b ReplicationEventPosition
		want int
	}{
		{pos("mysql-bin.000001", 4), pos("mysql-bin.000001", 4), 0},
		{pos("mysql-bin.000001", 4), pos("mysql-bin.000001", 900), -1},
		{pos("mysql-bin.000001", 900), pos("mysql-bin.000002", 4), -1},
		// #238: the six-digit boundary.
		{pos("mysql-bin.999999", 900), pos("mysql-bin.1000000", 4), -1},
		{pos("mysql-bin.100001", 4), pos("mysql-bin.1000000", 4), -1},
		{pos("mysql-bin.1000000", 4), pos("mysql-bin.999999", 4), 1},
		{pos("mysql-bin.9999999", 4), pos("mysql-bin.10000000", 4), -1},
		{pos("mysql-bin.2147483646", 4), pos("mysql-bin.2147483647", 4), -1},
		// Offsets only break a tie between equal files.
		{pos("mysql-bin.1000000", 4), pos("mysql-bin.999999", 4000000000), 1},
		// Different base names order by the base, as go-mysql does.
		{pos("a.000010", 4), pos("b.000009", 4), -1},
		// Names the server does not write sort before every numbered name.
		{pos("mysql-bin", 4), pos("mysql-bin.000001", 4), -1},
		{pos("mysql-bin.00000x", 4), pos("mysql-bin.000001", 4), -1},
		{pos("", 4), pos("mysql-bin.000001", 4), -1},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s:%d vs %s:%d", tt.a.Name, tt.a.Pos, tt.b.Name, tt.b.Pos), func(t *testing.T) {
			if got := tt.a.Compare(tt.b); got != tt.want {
				t.Fatalf("Compare = %d, want %d", got, tt.want)
			}
			if got := tt.b.Compare(tt.a); got != -tt.want {
				t.Fatalf("reverse Compare = %d, want %d", got, -tt.want)
			}
		})
	}
}

// TestReplicationEventPosition_CompareFollowsServerNaming checks Compare
// against the names the server writes, formatted the way it formats them
// ("%06lu"), around every point where the width grows.
func TestReplicationEventPosition_CompareFollowsServerNaming(t *testing.T) {
	name := func(n uint64) ReplicationEventPosition {
		return ReplicationEventPosition{Name: fmt.Sprintf("binlog.%06d", n), Pos: 4}
	}
	var seqs []uint64
	for _, edge := range []uint64{1, 999_999, 9_999_999, 99_999_999, 999_999_999, 2_147_483_646} {
		for d := uint64(0); d < 3 && edge+d <= 2_147_483_647; d++ {
			seqs = append(seqs, edge+d)
		}
	}
	for i, a := range seqs {
		for _, b := range seqs[i+1:] {
			if got := name(a).Compare(name(b)); got != -1 {
				t.Fatalf("Compare(%s, %s) = %d, want -1", name(a).Name, name(b).Name, got)
			}
		}
	}
}
