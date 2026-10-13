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
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/matryer/is"
)

func TestResolveLegacyValue(t *testing.T) {
	types := map[string]string{"a": "varchar", "b": "int", "d": "decimal", "e": "datetime", "f": "blob"}

	tests := []struct {
		column string
		in     string
		want   any
		ok     bool
	}{
		{"a", b64("tenant-1"), []byte("tenant-1"), true},
		{"d", b64("-200.00"), []byte("-200.00"), true},
		{"f", b64("\x00\xff"), []byte{0x00, 0xff}, true},
		{"e", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", true},
		{"b", b64("x"), nil, false}, // text for an INT column
		{"a", "***", nil, false},    // not base64
		{"gone", b64("x"), nil, false},
	}
	for _, tc := range tests {
		is := is.New(t)
		got, reason := resolveLegacyValue(common.LegacyKey(tc.in), tc.column, types)
		is.Equal(reason == "", tc.ok)
		if tc.ok {
			is.Equal(got, tc.want)
		}
	}
}

func TestResolveTablePosition(t *testing.T) {
	types := map[string]string{"a": "varchar", "b": "int", "c": "varchar", "d": "decimal", "e": "datetime"}

	t.Run("legacy composite VARCHAR/INT/VARCHAR restarts: byte-valued keys without a recorded type", func(t *testing.T) {
		is := is.New(t)
		legacy := common.TablePosition{MultipleKey: &common.TablePositionMultipleKey{
			{LastRead: common.LegacyKey(b64("tenant-1")), SnapshotEnd: common.LegacyKey(b64("tenant-9"))},
			{LastRead: int64(7), SnapshotEnd: int64(70)},
			{LastRead: common.LegacyKey(b64("x")), SnapshotEnd: common.LegacyKey(b64("z"))},
		}}
		_, reason := resolveTablePosition(legacy, common.PrimaryKeys{"a", "b", "c"}, types)
		is.True(reason != "")
	})

	t.Run("the same composite position with recorded types resolves exactly", func(t *testing.T) {
		is := is.New(t)
		pos := common.TablePosition{MultipleKey: &common.TablePositionMultipleKey{
			{KeyName: "a", LastRead: []byte("tenant-1"), KeyType: "varchar"},
			{KeyName: "b", LastRead: int64(7), KeyType: "int"},
			{KeyName: "c", LastRead: []byte("x"), KeyType: "varchar"},
		}}
		got, reason := resolveTablePosition(pos, common.PrimaryKeys{"a", "b", "c"}, types)
		is.Equal(reason, "")
		items := *got.MultipleKey
		is.Equal(items[0].LastRead, []byte("tenant-1"))
		is.Equal(items[2].KeyType, "varchar")
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

	t.Run("legacy single keys by type", func(t *testing.T) {
		tests := []struct {
			column string
			in     string
			ok     bool
		}{
			{"a", b64("x"), false}, // text, unrecorded: restart
			{"d", b64("-200.00"), false},
			{"e", "2020-01-01T00:00:00Z", true}, // temporal: still resumes
			{"b", b64("x"), false},
		}
		for _, tc := range tests {
			is := is.New(t)
			legacy := common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: common.LegacyKey(tc.in)}}
			_, reason := resolveTablePosition(legacy, common.PrimaryKeys{tc.column}, types)
			is.Equal(reason == "", tc.ok)
		}
	})

	t.Run("recorded single keys resume", func(t *testing.T) {
		is := is.New(t)
		pos := common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: []byte("-200.00"), KeyType: "decimal"}}
		got, reason := resolveTablePosition(pos, common.PrimaryKeys{"d"}, types)
		is.Equal(reason, "")
		is.Equal(got.SingleKey.LastRead, []byte("-200.00"))
	})

	t.Run("non-legacy values pass through", func(t *testing.T) {
		is := is.New(t)
		pos := common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: int64(9007199254740993)}}
		got, reason := resolveTablePosition(pos, common.PrimaryKeys{"b"}, types)
		is.Equal(reason, "")
		is.Equal(got.SingleKey.LastRead, int64(9007199254740993))
	})
}

func TestCheckKeyClass(t *testing.T) {
	types := map[string]string{"i": "int", "bi": "bigint", "u": "varchar", "d": "decimal", "t": "datetime", "j": "json", "b": "varbinary"}
	big := []byte("18446744073709551615") // BIGINT UNSIGNED above int64: the driver returns digits
	tests := []struct {
		name    string
		column  string
		v       any
		keyType string
		fits    bool
	}{
		{"int on int", "i", int64(1), "", true},
		{"uint on bigint", "bi", uint64(1), "", true},
		{"float on int", "i", float64(1), "", true},
		{"bigint unsigned above int64, unrecorded", "bi", big, "", true},
		{"bytes 10 on INT, unrecorded: the round 4 blocker", "i", []byte("10"), "", false},
		{"bytes 10 on BIGINT, unrecorded", "bi", []byte("10"), "", false},
		{"bytes 10 on INT, saved from a varchar key", "i", []byte("10"), "varchar", false},
		{"bytes 10 on INT, saved from a decimal key", "i", []byte("10"), "decimal", true},
		{"unsigned bigint digits, recorded", "bi", big, "bigint", true},
		{"non-numeric bytes on INT", "i", []byte("abc"), "", false},
		{"time on int", "i", time.Now(), "", false},
		{"bytes on varchar, unrecorded: restart", "u", []byte("a"), "", false},
		{"bytes on varchar, recorded", "u", []byte("a"), "varchar", true},
		{"string on varchar, recorded", "u", "a", "varchar", true},
		{"int on varchar", "u", int64(3), "", false},
		{"time on varchar", "u", time.Now(), "", false},
		{"decimal bytes saved from varchar: retype", "d", []byte("10"), "varchar", false},
		{"varchar bytes saved from decimal: retype", "u", []byte("10"), "decimal", false},
		{"decimal bytes on decimal", "d", []byte("1.00"), "decimal", true},
		{"decimal bytes on decimal, unrecorded: restart", "d", []byte("1.00"), "", false},
		{"int on decimal", "d", int64(1), "", true},
		{"text saved, binary now: different ordering", "b", []byte("a"), "varchar", false},
		{"binary saved, binary now", "b", []byte("a"), "varbinary", true},
		{"binary bytes unrecorded: restart", "b", []byte("a"), "", false},
		{"char saved, enum-or-set tolerated", "u", []byte("a"), "char", true},
		{"datetime on datetime", "t", time.Now(), "datetime", true},
		{"datetime text on datetime", "t", "2020-01-01T00:00:00Z", "", true},
		{"int on datetime", "t", int64(1), "", false},
		{"bytes on datetime", "t", []byte("x"), "", false},
		{"unknown class is not checked", "j", int64(1), "", true},
		{"no saved key yet", "i", nil, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			is.Equal(checkKeyClass(tc.v, tc.column, tc.keyType, types) == "", tc.fits)
		})
	}
}
