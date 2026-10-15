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

const (
	// CDCPositionVersion is stamped on cdc_position envelopes.
	CDCPositionVersion = 1

	// SnapshotPositionVersion is stamped on snapshot_position envelopes. It is 2
	// because per-table keys are written type-exactly (see keyvalue.go), a form
	// version 1 readers cannot interpret: they refuse the position (see
	// ErrPositionVersionUnsupported) instead of seeking on a misread key. CDC
	// positions are unaffected, so a downgrade is refused only while a snapshot
	// is in progress.
	SnapshotPositionVersion = 2

	// TypesPositionVersion is stamped on every snapshot and CDC envelope
	// written in types.mode=strict. Such an envelope carries Types; a build
	// that predates it refuses the position (ErrPositionVersionUnsupported)
	// instead of resuming it with legacy record shapes. Positions written in
	// legacy mode keep versions 1 and 2. See
	// ConduitIO/conduit docs/design-documents/20261012-mysql-type-semantics.md.
	TypesPositionVersion = 3

	// CurrentPositionVersion is the highest envelope version this build reads.
	// Bump it whenever a position-format change is not safely readable by the
	// previous reader. Absent/0 on read means a legacy pre-version position
	// (treat as readable, see ParseSDKPosition).
	CurrentPositionVersion = TypesPositionVersion
)

// TypesStamp is the resolved type configuration a position was written
// under. nil (no "types" object) means legacy. Every setting is listed, so a
// later version that adds one can tell "the stamp predates it" from a choice.
type TypesStamp struct {
	Mode           string `json:"mode"`
	Decimal        string `json:"decimal"`
	BigintUnsigned string `json:"bigint_unsigned"`
}

// Name is the stamp's name as operators see and type it, for example
// "strict/string/long". nil is "legacy".
func (t *TypesStamp) Name() string {
	if t == nil {
		return "legacy"
	}
	return t.Mode + "/" + t.Decimal + "/" + t.BigintUnsigned
}

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
	Version int `json:"version,omitempty"`
	// Types is the type configuration the position was written under; nil
	// for legacy. Only version TypesPositionVersion envelopes carry it.
	Types            *TypesStamp       `json:"types,omitempty"`
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

// ToSDKPosition marshals the position. It fails, rather than writing a lossy
// position, when a table's last-read key has a Go type that cannot be encoded
// type-exactly (see keyvalue.go).
func (p SnapshotPosition) ToSDKPosition() (opencdc.Position, error) {
	return p.ToSDKPositionWithTypes(nil)
}

// ToSDKPositionWithTypes marshals the position with a type stamp; a nil
// stamp writes the legacy envelope (version 2).
func (p SnapshotPosition) ToSDKPositionWithTypes(types *TypesStamp) (opencdc.Position, error) {
	version := SnapshotPositionVersion
	if types != nil {
		version = TypesPositionVersion
	}
	v, err := json.Marshal(Position{Version: version, Types: types, SnapshotPosition: &p})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot position: %w", err)
	}
	return v, nil
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
		if p.CDCStart.ReplayFrom != nil {
			replay := *p.CDCStart.ReplayFrom
			cdcStart.ReplayFrom = &replay
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

	if pos.Version >= TypesPositionVersion && pos.Types == nil {
		return pos, fmt.Errorf("%w: a version %d position must carry its type stamp and this one has none",
			ErrPositionVersionUnsupported, pos.Version)
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

	// KeyType is the MySQL type of the key column when the position was
	// written, lowercase and without "unsigned" (for example "varchar",
	// "decimal"). It lets a resume tell that the column has since been retyped
	// to a type with a different ordering, which the key value alone cannot show
	// (the bytes "10" are both a VARCHAR and a DECIMAL key). Empty in positions
	// written without it; additive, ignored by readers that do not know it.
	KeyType string `json:"key_type,omitempty"`
}

type TablePositionMultipleKey []TablePositionMultipleKeyItem

type TablePositionMultipleKeyItem struct {
	KeyName     string `json:"key_name"`
	LastRead    any    `json:"last_read"`
	SnapshotEnd any    `json:"snapshot_end"`

	// KeyType: see TablePositionSingleKey.KeyType.
	KeyType string `json:"key_type,omitempty"`
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

	// ReplayFrom is where a restart starts reading the binlog: the position
	// canal had last synced when the event was handled (after the previous
	// transaction's commit, after a BEGIN, or a rotation), so the Table_map
	// events of the event's statement are read again and the event can be
	// decoded. A restart skips every event before ReplicationEventPosition and
	// the rows of that event up to Index. nil in positions written before this
	// field existed; see the cdc iterator for how those resume. Additive:
	// older builds ignore it and resume from PrevPosition as before.
	ReplayFrom *ReplicationEventPosition `json:"replay_from,omitempty"`

	// TableCoverage records which tables' existing rows the pipeline has
	// covered; see TableCoverage. nil means a position written before this field
	// existed. Additive: the position version is intentionally not bumped, so
	// older builds ignore the field and drop it on their next write. See
	// docs/design-documents/20261012-cdc-snapshotted-tables.md.
	TableCoverage *TableCoverage `json:"table_coverage,omitempty"`
}

// Compare orders two binlog positions: by file name (binlog file names have a
// fixed-width sequence number, so they sort as text), then by offset.
func (p ReplicationEventPosition) Compare(o ReplicationEventPosition) int {
	switch {
	case p.Name < o.Name:
		return -1
	case p.Name > o.Name:
		return 1
	case p.Pos < o.Pos:
		return -1
	case p.Pos > o.Pos:
		return 1
	}
	return 0
}

func (p ReplicationEventPosition) ToMysqlPos() mysql.Position {
	return mysql.Position{
		Name: p.Name,
		Pos:  p.Pos,
	}
}

func (p CdcPosition) ToSDKPosition() opencdc.Position {
	return p.ToSDKPositionWithTypes(nil)
}

// ToSDKPositionWithTypes marshals the position with a type stamp; a nil
// stamp writes the legacy envelope (version 1).
func (p CdcPosition) ToSDKPositionWithTypes(types *TypesStamp) opencdc.Position {
	version := CDCPositionVersion
	if types != nil {
		version = TypesPositionVersion
	}
	v, err := json.Marshal(Position{Version: version, Types: types, CdcPosition: &p})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}
