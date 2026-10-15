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

//go:build chaos

package chaos

import (
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
)

func TestCDCLess(t *testing.T) {
	at := func(name string, pos uint32, idx int) *common.CdcPosition {
		return &common.CdcPosition{ReplicationEventPosition: common.ReplicationEventPosition{Name: name, Pos: pos}, Index: idx}
	}
	compressed := func(replay uint32, ordinal, idx int) *common.CdcPosition {
		p := at("binlog.000002", 0, idx)
		p.ReplayFrom = &common.ReplicationEventPosition{Name: "binlog.000002", Pos: replay}
		p.Compressed = &common.CompressedEventRef{Ordinal: ordinal, Tables: "fp"}
		return p
	}
	// Each position is strictly after the one before it.
	ordered := []*common.CdcPosition{
		at("binlog.000001", 900, 3),
		at("binlog.000002", 500, 0),
		at("binlog.000002", 500, 1),
		compressed(700, 1, 0), // a compressed transaction starting after the event at 500
		compressed(700, 1, 4),
		compressed(700, 2, 0), // a later rows event of the same transaction
		at("binlog.000002", 1200, 0),
		compressed(1500, 1, 0),
		at("binlog.000003", 4, 0),
	}
	for i := range ordered {
		for j := range ordered {
			if got, want := cdcLess(ordered[i], ordered[j]), i < j; got != want {
				t.Errorf("cdcLess(%d, %d) = %v, want %v", i, j, got, want)
			}
		}
	}
}
