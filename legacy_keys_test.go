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
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/matryer/is"
)

func TestResolveTablePosition(t *testing.T) {
	types := map[string]string{"a": "varchar", "b": "int", "c": "varchar", "d": "decimal", "e": "datetime", "f": "blob"}

	t.Run("legacy composite VARCHAR/INT/VARCHAR with empty key names", func(t *testing.T) {
		is := is.New(t)
		// What the previous build persisted: key_name never filled in, text as base64,
		// numbers as numbers.
		legacy := common.TablePosition{MultipleKey: &common.TablePositionMultipleKey{
			{LastRead: common.LegacyKey(b64("tenant-1")), SnapshotEnd: common.LegacyKey(b64("tenant-9"))},
			{LastRead: int64(7), SnapshotEnd: int64(70)},
			{LastRead: common.LegacyKey(b64("x")), SnapshotEnd: common.LegacyKey(b64("z"))},
		}}

		got, reason := resolveTablePosition(legacy, common.PrimaryKeys{"a", "b", "c"}, types)
		is.Equal(reason, "")
		items := *got.MultipleKey
		is.Equal(items[0].KeyName, "a")
		is.Equal(items[0].LastRead, []byte("tenant-1"))
		is.Equal(items[1].LastRead, int64(7))
		is.Equal(items[2].KeyName, "c")
		is.Equal(items[2].LastRead, []byte("x"))
	})

	t.Run("composite key columns changed: restart", func(t *testing.T) {
		is := is.New(t)
		legacy := common.TablePosition{MultipleKey: &common.TablePositionMultipleKey{
			{LastRead: common.LegacyKey(b64("x"))}, {LastRead: int64(1)},
		}}
		_, reason := resolveTablePosition(legacy, common.PrimaryKeys{"a", "b", "c"}, types)
		is.True(reason != "")
	})

	t.Run("unknown column: restart", func(t *testing.T) {
		is := is.New(t)
		legacy := common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: common.LegacyKey(b64("x"))}}
		_, reason := resolveTablePosition(legacy, common.PrimaryKeys{"gone"}, types)
		is.True(reason != "")
	})

	t.Run("single keys by type", func(t *testing.T) {
		tests := []struct {
			column string
			in     string
			want   any
			ok     bool
		}{
			{"d", b64("-200.00"), []byte("-200.00"), true},
			{"f", b64("\x00\xff"), []byte{0x00, 0xff}, true},
			{"e", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", true},
			{"b", b64("x"), nil, false}, // text for an INT column
			{"a", "***", nil, false},    // not base64
		}
		for _, tc := range tests {
			is := is.New(t)
			legacy := common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: common.LegacyKey(tc.in)}}
			got, reason := resolveTablePosition(legacy, common.PrimaryKeys{tc.column}, types)
			is.Equal(reason == "", tc.ok)
			if tc.ok {
				is.Equal(got.SingleKey.LastRead, tc.want)
			}
		}
	})

	t.Run("non-legacy values pass through", func(t *testing.T) {
		is := is.New(t)
		pos := common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: int64(9007199254740993)}}
		got, reason := resolveTablePosition(pos, common.PrimaryKeys{"b"}, types)
		is.Equal(reason, "")
		is.Equal(got.SingleKey.LastRead, int64(9007199254740993))
	})
}
