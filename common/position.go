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
	"fmt"
	"maps"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type PositionType string

const (
	PositionTypeSnapshot PositionType = "snapshot"
	PositionTypeCDC      PositionType = "cdc"
)

// CurrentPositionVersion is the version stamped on every position this connector
// writes. Bump it whenever a position-format change is not safely readable by the
// previous reader. Absent/0 on read means a legacy pre-version position (treat as
// readable, see ParseSDKPosition).
const CurrentPositionVersion = 1

// ErrPositionVersionUnsupported is returned by ParseSDKPosition when a persisted
// position's envelope version is higher than the highest version this connector
// build knows how to read. This makes a downgrade (running an older connector
// build against a position written by a newer one) a loud, actionable failure
// instead of a silent mis-read. See docs/design-documents/20260724-snapshot-cdc-position-handoff.md.
var ErrPositionVersionUnsupported = errors.New("position version unsupported")

type Position struct {
	// Version is the position-envelope format version. 0/absent means a legacy
	// position written before this field existed; it is treated as readable. A
	// version greater than CurrentPositionVersion is refused by ParseSDKPosition
	// (see ErrPositionVersionUnsupported) rather than silently parsed, so that a
	// future downgrade is detectable.
	Version          int               `json:"version,omitempty"`
	SnapshotPosition *SnapshotPosition `json:"snapshot_position,omitempty"`
	CdcPosition      *CdcPosition      `json:"cdc_position,omitempty"`
}

type SnapshotPosition struct {
	Snapshots SnapshotPositions `json:"snapshots,omitempty"`

	// CDCStart is the server-wide binlog position (P0) captured under the read
	// lock at snapshot start. It is stamped on every snapshot record so that a
	// mid-snapshot restart resumes CDC from P0 rather than a fresh master
	// position, closing the (P0, P1] data-loss window described in
	// docs/design-documents/20260724-snapshot-cdc-position-handoff.md. It is nil
	// for positions written before this field existed and for steady-state CDC
	// positions.
	CDCStart *CdcPosition `json:"cdc_start,omitempty"`
}

func (p SnapshotPosition) ToSDKPosition() opencdc.Position {
	v, err := json.Marshal(Position{Version: CurrentPositionVersion, SnapshotPosition: &p})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

// Clone deep-copies the position, including CDCStart. This is load-bearing: the
// resumed snapshot's lastPosition is derived from Clone() (see
// newSnapshotIterator, setupWorkers), and every record it emits carries
// lastPosition's CDCStart. If Clone dropped CDCStart, records emitted during a
// *resumed* snapshot would carry no cdc_start, and a second mid-snapshot crash
// would reintroduce the original data-loss bug (Invariant 3).
func (p SnapshotPosition) Clone() SnapshotPosition {
	var newPosition SnapshotPosition
	newPosition.Snapshots = make(SnapshotPositions)
	maps.Copy(newPosition.Snapshots, p.Snapshots)

	if p.CDCStart != nil {
		cdcStart := *p.CDCStart
		if p.CDCStart.PrevPosition != nil {
			prev := *p.CDCStart.PrevPosition
			cdcStart.PrevPosition = &prev
		}
		newPosition.CDCStart = &cdcStart
	}

	return newPosition
}

// ParseSDKPosition is the single parse chokepoint for positions read back from
// Conduit. It enforces the Version envelope gate: a position written by a future,
// higher-versioned connector build is refused rather than silently parsed, since
// this build cannot know whether doing so is safe. See ErrPositionVersionUnsupported.
func ParseSDKPosition(p opencdc.Position) (Position, error) {
	var pos Position
	if err := json.Unmarshal(p, &pos); err != nil {
		return pos, fmt.Errorf("failed to parse position: %w", err)
	}

	if pos.Version > CurrentPositionVersion {
		return pos, fmt.Errorf(
			"%w: position was written with version %d, this connector build supports up to version %d; "+
				"upgrade the connector to read this position, or clear the connector's position to start a fresh snapshot",
			ErrPositionVersionUnsupported, pos.Version, CurrentPositionVersion)
	}

	return pos, nil
}

// SnapshotPositions represents the current snapshot status of every table
// that has been snapshotted.
type SnapshotPositions map[string]TablePosition

type TablePosition struct {
	SingleKey   *TablePositionSingleKey
	MultipleKey *TablePositionMultipleKey
}

type TablePositionSingleKey struct {
	LastRead    any `json:"last_read"`
	SnapshotEnd any `json:"snapshot_end"`
}

type TablePositionMultipleKey []TablePositionMultipleKeyItem

type TablePositionMultipleKeyItem struct {
	KeyName     string `json:"key_name"`
	LastRead    any    `json:"last_read"`
	SnapshotEnd any    `json:"snapshot_end"`
}

type ReplicationEventPosition struct {
	// Name represents the mysql binlog filename.
	Name string `json:"name"`
	Pos  uint32 `json:"pos"`
}

type CdcPosition struct {
	ReplicationEventPosition

	// Index represents the row index in the mysql replication event.
	Index int `json:"idx,omitempty"`

	// PrevPosition represents position of the mysql replication
	// event just before the current one.
	PrevPosition *ReplicationEventPosition `json:"prev,omitempty"`
}

func (p ReplicationEventPosition) ToMysqlPos() mysql.Position {
	return mysql.Position{
		Name: p.Name,
		Pos:  p.Pos,
	}
}

func (p CdcPosition) ToSDKPosition() opencdc.Position {
	v, err := json.Marshal(Position{Version: CurrentPositionVersion, CdcPosition: &p})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}
