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
	"testing"

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
	is.Equal(raw["version"], float64(CurrentPositionVersion))

	parsed, err := ParseSDKPosition(sdkPos)
	is.NoErr(err)
	is.Equal(parsed.Version, CurrentPositionVersion)
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
	is.Equal(parsed.Version, CurrentPositionVersion)
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
	is.Equal(nPlus1Parsed.Version, CurrentPositionVersion)
	is.True(nPlus1Parsed.SnapshotPosition.CDCStart != nil)
	is.Equal(nPlus1Parsed.SnapshotPosition.CDCStart.Name, "binlog.000001")
	is.Equal(nPlus1Parsed.SnapshotPosition.CDCStart.Pos, uint32(4))
}
