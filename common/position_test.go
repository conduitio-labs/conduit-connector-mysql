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
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// legacySnapshotPosition and legacyPosition model the pre-fix (N) wire shape:
// no envelope Version, no SnapshotPosition.CDCStart. Used to prove wire
// compatibility both ways across the position-format change (see
// docs/design-documents/20260724-snapshot-cdc-position-handoff.md, "Position
// format change and migration").
type legacySnapshotPosition struct {
	Snapshots SnapshotPositions `json:"snapshots,omitempty"`
}

type legacyPosition struct {
	SnapshotPosition *legacySnapshotPosition `json:"snapshot_position,omitempty"`
	CdcPosition      *CdcPosition            `json:"cdc_position,omitempty"`
}

func TestPosition_VersionRoundTrip(t *testing.T) {
	is := is.New(t)

	cdcPos := CdcPosition{
		ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000001", Pos: 42},
	}

	sdkPos := cdcPos.ToSDKPosition()

	var raw map[string]any
	is.NoErr(json.Unmarshal(sdkPos, &raw))
	is.Equal(raw["version"], float64(CDCPositionVersion))

	parsed, err := ParseSDKPosition(sdkPos)
	is.NoErr(err)
	is.Equal(parsed.Version, CDCPositionVersion)
	is.True(parsed.CdcPosition != nil)
	is.Equal(*parsed.CdcPosition, cdcPos)
}

func TestPosition_SnapshotVersionRoundTrip(t *testing.T) {
	is := is.New(t)

	snapshotPos := SnapshotPosition{
		Snapshots: SnapshotPositions{
			"users": TablePosition{SingleKey: &TablePositionSingleKey{LastRead: float64(10), SnapshotEnd: float64(100)}},
		},
		CDCStart: &CdcPosition{
			ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000001", Pos: 4},
		},
	}

	sdkPos := snapshotPos.ToSDKPosition()

	parsed, err := ParseSDKPosition(sdkPos)
	is.NoErr(err)
	is.Equal(parsed.Version, SnapshotPositionVersion)
	is.True(parsed.SnapshotPosition != nil)
	is.True(parsed.SnapshotPosition.CDCStart != nil)
	is.Equal(*parsed.SnapshotPosition.CDCStart, *snapshotPos.CDCStart)
}

func TestPosition_VersionRefusal(t *testing.T) {
	is := is.New(t)

	// A position written by a hypothetical future connector build with a higher
	// envelope version than this build supports.
	future := Position{
		Version: CurrentPositionVersion + 1,
		CdcPosition: &CdcPosition{
			ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000009", Pos: 1},
		},
	}

	raw, err := json.Marshal(future)
	is.NoErr(err)

	_, err = ParseSDKPosition(raw)
	is.True(err != nil)
	is.True(errors.Is(err, ErrPositionVersionUnsupported))
}

func TestPosition_LegacyVersionIsZeroAndOK(t *testing.T) {
	is := is.New(t)

	// version = 0 (absent) must still parse without error: legacy pre-version
	// positions are readable, not refused.
	pos := Position{
		CdcPosition: &CdcPosition{
			ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000001", Pos: 1},
		},
	}
	raw, err := json.Marshal(pos)
	is.NoErr(err)

	var rawMap map[string]any
	is.NoErr(json.Unmarshal(raw, &rawMap))
	_, hasVersion := rawMap["version"]
	is.True(!hasVersion) // omitempty must drop a zero version

	parsed, err := ParseSDKPosition(raw)
	is.NoErr(err)
	is.Equal(parsed.Version, 0)
}

func TestSnapshotPosition_Clone_PreservesCDCStart(t *testing.T) {
	is := is.New(t)

	original := SnapshotPosition{
		Snapshots: SnapshotPositions{
			"users": TablePosition{SingleKey: &TablePositionSingleKey{LastRead: float64(5), SnapshotEnd: float64(50)}},
		},
		CDCStart: &CdcPosition{
			ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000001", Pos: 100},
			PrevPosition:             &ReplicationEventPosition{Name: "binlog.000001", Pos: 50},
		},
	}

	clone := original.Clone()

	is.True(clone.CDCStart != nil)
	is.Equal(*clone.CDCStart, *original.CDCStart)

	// Must be a deep copy: mutating the clone's CDCStart must not affect the
	// original. This is the load-bearing property from the design doc - if
	// Clone() aliased the pointer instead of copying, a second mid-snapshot
	// crash could reintroduce the original data-loss bug.
	clone.CDCStart.Pos = 999
	clone.CDCStart.PrevPosition.Pos = 999
	is.Equal(original.CDCStart.Pos, uint32(100))
	is.Equal(original.CDCStart.PrevPosition.Pos, uint32(50))

	// Snapshots must also remain independently mutable (pre-existing behavior).
	clone.Snapshots["users"] = TablePosition{}
	_, stillPresent := original.Snapshots["users"]
	is.True(stillPresent)
}

func TestSnapshotPosition_Clone_NilCDCStart(t *testing.T) {
	is := is.New(t)

	original := SnapshotPosition{Snapshots: SnapshotPositions{}}
	clone := original.Clone()
	is.True(clone.CDCStart == nil)
}

// TestPosition_ForwardCompat_NReadsNPlus1 proves an N-shaped (pre-fix) position
// reader tolerates a position written by this (N+1) connector: the unknown
// version and cdc_start keys are ignored, snapshot fields decode intact. This is
// the "N reads N+1's position" direction from the migration section.
func TestPosition_ForwardCompat_NReadsNPlus1(t *testing.T) {
	is := is.New(t)

	nPlus1 := SnapshotPosition{
		Snapshots: SnapshotPositions{
			"users": TablePosition{SingleKey: &TablePositionSingleKey{LastRead: float64(7), SnapshotEnd: float64(70)}},
		},
		CDCStart: &CdcPosition{ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000002", Pos: 8}},
	}
	raw := nPlus1.ToSDKPosition()

	var legacy legacyPosition
	is.NoErr(json.Unmarshal(raw, &legacy))
	is.True(legacy.SnapshotPosition != nil)
	is.Equal(legacy.SnapshotPosition.Snapshots, nPlus1.Snapshots)
}

// TestPosition_BackwardCompat_NPlus1ReadsN proves this (N+1) connector tolerates
// a position written by the pre-fix (N) connector: CDCStart and Version unmarshal
// to their zero values, and the restart-seeding fallback (see source.go) treats
// that exactly like a fresh start. This is the "N+1 reads N's position" direction.
func TestPosition_BackwardCompat_NPlus1ReadsN(t *testing.T) {
	is := is.New(t)

	n := legacyPosition{
		SnapshotPosition: &legacySnapshotPosition{
			Snapshots: SnapshotPositions{
				"users": TablePosition{SingleKey: &TablePositionSingleKey{LastRead: float64(3), SnapshotEnd: float64(30)}},
			},
		},
	}
	raw, err := json.Marshal(n)
	is.NoErr(err)

	parsed, err := ParseSDKPosition(raw)
	is.NoErr(err)
	is.Equal(parsed.Version, 0)
	is.True(parsed.SnapshotPosition != nil)
	is.True(parsed.SnapshotPosition.CDCStart == nil)
	is.Equal(parsed.SnapshotPosition.Snapshots, n.SnapshotPosition.Snapshots)
}

// TestPosition_GoldenFile_RoundTrip guards against accidental field renames by
// decoding fixed, hand-written JSON blobs representing a stored N position and a
// stored N+1 position.
func TestPosition_GoldenFile_RoundTrip(t *testing.T) {
	is := is.New(t)

	const nJSON = `{"snapshot_position":{"snapshots":{"users":{"SingleKey":{"last_read":5,"snapshot_end":50}}}}}`
	const nPlus1JSON = `{"version":1,"snapshot_position":{"snapshots":{"users":{"SingleKey":{"last_read":5,"snapshot_end":50}}},"cdc_start":{"name":"binlog.000001","pos":4}}}`

	nParsed, err := ParseSDKPosition([]byte(nJSON))
	is.NoErr(err)
	is.Equal(nParsed.Version, 0)
	is.True(nParsed.SnapshotPosition.CDCStart == nil)

	nPlus1Parsed, err := ParseSDKPosition([]byte(nPlus1JSON))
	is.NoErr(err)
	is.Equal(nPlus1Parsed.Version, 1) // a version 1 snapshot position is still read
	is.True(nPlus1Parsed.SnapshotPosition.CDCStart != nil)
	is.Equal(nPlus1Parsed.SnapshotPosition.CDCStart.Name, "binlog.000001")
	is.Equal(nPlus1Parsed.SnapshotPosition.CDCStart.Pos, uint32(4))
}

// oldCdcPosition is CdcPosition as the previous build defined it (no
// table_coverage). A build that does not know the field must read, and
// preserve the meaning of, a position that carries it.
type oldPosition struct {
	Version     int `json:"version,omitempty"`
	CdcPosition *struct {
		ReplicationEventPosition
		Index        int                       `json:"idx,omitempty"`
		PrevPosition *ReplicationEventPosition `json:"prev,omitempty"`
	} `json:"cdc_position,omitempty"`
}

func TestCdcPosition_TableCoverage_AdditiveCompat(t *testing.T) {
	t.Run("N+1 writes, N reads", func(t *testing.T) {
		is := is.New(t)
		raw := CdcPosition{
			ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000001", Pos: 9},
			Index:                    1,
			TableCoverage:            NewTableCoverage([]string{"a", "b"}, nil, nil),
		}.ToSDKPosition()

		var old oldPosition
		is.NoErr(json.Unmarshal(raw, &old))
		is.Equal(old.Version, 1) // version must not be bumped: older builds would refuse it
		is.Equal(old.CdcPosition.Name, "binlog.000001")
		is.Equal(old.CdcPosition.Pos, uint32(9))
		is.Equal(old.CdcPosition.Index, 1)
	})

	t.Run("N writes, N+1 reads: legacy position has unknown coverage", func(t *testing.T) {
		is := is.New(t)
		legacy := opencdc.Position(`{"version":1,"cdc_position":{"name":"binlog.000004","pos":120,"idx":2,"prev":{"name":"binlog.000004","pos":80}}}`)
		parsed, err := ParseSDKPosition(legacy)
		is.NoErr(err)
		is.True(parsed.CdcPosition.TableCoverage == nil)
		is.Equal(parsed.CdcPosition.Pos, uint32(120))
	})

	t.Run("known-empty is distinct from absent", func(t *testing.T) {
		is := is.New(t)
		raw := CdcPosition{TableCoverage: NewTableCoverage(nil, nil, nil)}.ToSDKPosition()
		is.True(strings.Contains(string(raw), `"table_coverage":{}`))
		parsed, err := ParseSDKPosition(raw)
		is.NoErr(err)
		is.True(parsed.CdcPosition.TableCoverage != nil)
		is.True(!parsed.CdcPosition.TableCoverage.Hashed())
	})
}

// previousMaxPositionVersion is CurrentPositionVersion of the build before keys
// were written type-exactly.
const previousMaxPositionVersion = 1

func TestSnapshotPosition_KeysRoundTripExactly(t *testing.T) {
	ts := time.Date(2024, 2, 29, 13, 14, 15, 123456000, time.UTC)

	tests := []struct {
		name string
		key  any
	}{
		{"int64 above 2^53", int64(9007199254740993)},
		{"int64 negative", int64(-9007199254740995)},
		{"int64 max", int64(math.MaxInt64)},
		{"uint64 above int64", uint64(1 << 63)},
		{"uint64 max", uint64(math.MaxUint64)},
		{"negative decimal bytes", []byte("-300.00")},
		{"decimal bytes", []byte("12345678901234567890.123456789")},
		{"text bytes", []byte("caf\xc3\xa9")},
		{"binary bytes", []byte{0x00, 0xff, 0x80}},
		{"empty bytes", []byte{}},
		{"string", "plain"},
		{"float64", 0.1},
		{"float32", float32(0.1)},
		{"time", ts},
		{"bool", true},
		{"nil", nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			in := SnapshotPosition{Snapshots: SnapshotPositions{
				"t": {SingleKey: &TablePositionSingleKey{LastRead: tc.key, SnapshotEnd: tc.key}},
				"c": {MultipleKey: &TablePositionMultipleKey{{KeyName: "a", LastRead: tc.key, SnapshotEnd: tc.key}}},
			}}

			parsed, err := ParseSDKPosition(in.ToSDKPosition())
			is.NoErr(err)

			got := parsed.SnapshotPosition.Snapshots
			is.Equal(got["t"].SingleKey.LastRead, tc.key)
			is.Equal(got["t"].SingleKey.SnapshotEnd, tc.key)
			is.Equal((*got["c"].MultipleKey)[0].LastRead, tc.key)
			is.Equal((*got["c"].MultipleKey)[0].KeyName, "a")
		})
	}
}

func TestSnapshotPosition_UnsupportedKeyType(t *testing.T) {
	is := is.New(t)
	_, err := json.Marshal(TablePositionSingleKey{LastRead: struct{}{}})
	is.True(err != nil)
}

// Positions written before the tagged form: bare scalars. Integers must now
// be read exactly (they were always written exactly, and read back as float64).
func TestSnapshotPosition_LegacyKeysStillDecode(t *testing.T) {
	tests := []struct {
		name string
		json string
		want any
	}{
		{"small int", `{"snapshot_position":{"snapshots":{"t":{"SingleKey":{"last_read":5,"snapshot_end":50}}}}}`, int64(5)},
		{"int above 2^53, exact", `{"version":1,"snapshot_position":{"snapshots":{"t":{"SingleKey":{"last_read":9007199254740993,"snapshot_end":9007199254740999}}}}}`, int64(9007199254740993)},
		{"uint above int64, exact", `{"version":1,"snapshot_position":{"snapshots":{"t":{"SingleKey":{"last_read":18446744073709551615,"snapshot_end":1}}}}}`, uint64(math.MaxUint64)},
		{"float", `{"version":1,"snapshot_position":{"snapshots":{"t":{"SingleKey":{"last_read":1.5,"snapshot_end":2.5}}}}}`, 1.5},
		{"string", `{"version":1,"snapshot_position":{"snapshots":{"t":{"SingleKey":{"last_read":"abc","snapshot_end":"abd"}}}}}`, "abc"},
		{"null last_read", `{"version":1,"snapshot_position":{"snapshots":{"t":{"SingleKey":{"last_read":null,"snapshot_end":3}}}}}`, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			parsed, err := ParseSDKPosition([]byte(tc.json))
			is.NoErr(err)
			is.Equal(parsed.SnapshotPosition.Snapshots["t"].SingleKey.LastRead, tc.want)
		})
	}

	t.Run("legacy composite key", func(t *testing.T) {
		is := is.New(t)
		parsed, err := ParseSDKPosition([]byte(`{"version":1,"snapshot_position":{"snapshots":{"c":{"MultipleKey":[{"key_name":"a","last_read":2,"snapshot_end":4},{"key_name":"b","last_read":3,"snapshot_end":9}]}}}}`))
		is.NoErr(err)
		mk := *parsed.SnapshotPosition.Snapshots["c"].MultipleKey
		is.Equal(len(mk), 2)
		is.Equal(mk[1].KeyName, "b")
		is.Equal(mk[1].LastRead, int64(3))
	})
}

func TestSnapshotPosition_UnknownKeyEncodingIsAnError(t *testing.T) {
	is := is.New(t)
	_, err := ParseSDKPosition([]byte(`{"version":2,"snapshot_position":{"snapshots":{"t":{"SingleKey":{"last_read":{"t":"hologram","v":"1"},"snapshot_end":null}}}}}`))
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "hologram"))
}

// A build that predates the tagged form refuses a snapshot position written by
// this one (loud), and this build still writes CDC positions that build reads.
func TestPosition_DowngradeBehaviour(t *testing.T) {
	is := is.New(t)

	snap := SnapshotPosition{Snapshots: SnapshotPositions{"t": {SingleKey: &TablePositionSingleKey{LastRead: int64(1), SnapshotEnd: int64(2)}}}}
	var env struct {
		Version int `json:"version"`
	}
	is.NoErr(json.Unmarshal(snap.ToSDKPosition(), &env))
	is.True(env.Version > previousMaxPositionVersion) // the previous build refuses it

	is.NoErr(json.Unmarshal(CdcPosition{}.ToSDKPosition(), &env))
	is.True(env.Version <= previousMaxPositionVersion) // and still reads cdc positions
}
