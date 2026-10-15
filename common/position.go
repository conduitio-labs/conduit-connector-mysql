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
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

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

	// CurrentPositionVersion is the highest envelope version this build reads.
	// Bump it whenever a position-format change is not safely readable by the
	// previous reader. Absent/0 on read means a legacy pre-version position
	// (treat as readable, see ParseSDKPosition).
	CurrentPositionVersion = SnapshotPositionVersion
)

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

// ToSDKPosition marshals the position. It fails, rather than writing a lossy
// position, when a table's last-read key has a Go type that cannot be encoded
// type-exactly (see keyvalue.go).
func (p SnapshotPosition) ToSDKPosition() (opencdc.Position, error) {
	v, err := json.Marshal(Position{Version: SnapshotPositionVersion, SnapshotPosition: &p})
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
		if p.CDCStart.Compressed != nil {
			compressed := *p.CDCStart.Compressed
			cdcStart.Compressed = &compressed
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

	// Compressed locates an event inside a compressed transaction
	// (binlog_transaction_compression=ON). The events of such a transaction
	// carry no offset of their own (ReplicationEventPosition.Pos is 0), so
	// the event is named by its place after ReplayFrom instead, which is then
	// the start of the compressed transaction. nil for events with an offset
	// and in positions written before this field existed; see the cdc
	// iterator for how those resume. Additive: older builds ignore it.
	Compressed *CompressedEventRef `json:"compressed,omitempty"`

	// TableCoverage records which tables' existing rows the pipeline has
	// covered; see TableCoverage. nil means a position written before this field
	// existed. Additive: the position version is intentionally not bumped, so
	// older builds ignore the field and drop it on their next write. See
	// docs/design-documents/20261012-cdc-snapshotted-tables.md.
	TableCoverage *TableCoverage `json:"table_coverage,omitempty"`
}

// CompressedEventRef names a rows event inside a compressed transaction by
// counting from CdcPosition.ReplayFrom.
type CompressedEventRef struct {
	// Ordinal is the event's 1-based number among the rows events without an
	// offset that the connector received after ReplayFrom. Only events of
	// captured tables reach the connector, so the count depends on the table
	// set; see Tables.
	Ordinal int `json:"ordinal"`
	// Tables fingerprints the captured table set Ordinal was counted under. A
	// restart with another set cannot trust Ordinal and delivers the whole
	// transaction again.
	Tables string `json:"tables"`
}

// Compare orders two binlog positions the way the server writes them: by
// file, then by offset within the file.
//
// Files are ordered by base name, then by the number after the last ".",
// compared as a number. MySQL and MariaDB pad that number to at least six
// digits and keep counting past 999999 up to 2^31-1 (find_uniq_filename,
// "%06lu", MAX_LOG_UNIQUE_FN_EXT; mysql-server sql/binlog.cc, MariaDB
// sql/log.cc), so "mysql-bin.1000000" follows "mysql-bin.999999" although it
// sorts before it as text. Comparing names as text skipped records on a
// resume across that rotation (#238).
//
// Invariant 3: this ordering decides which re-read events a restart drops as
// already delivered (cdcEventHandler.onRows, passedTarget), so it must match
// the server's order.
//
// A name without a numeric suffix, which the server does not write, is
// compared as text, as before. go-mysql's CompareBinlogFileName is not used
// because it panics on such a name, and a position is read from storage.
func (p ReplicationEventPosition) Compare(o ReplicationEventPosition) int {
	if c := compareBinlogFileNames(p.Name, o.Name); c != 0 {
		return c
	}
	return cmp.Compare(p.Pos, o.Pos)
}

// compareBinlogFileNames orders binlog file names by base name, then by
// sequence number; see ReplicationEventPosition.Compare.
func compareBinlogFileNames(a, b string) int {
	if a == b {
		return 0
	}
	aBase, aSeq, aOK := splitBinlogFileName(a)
	bBase, bSeq, bOK := splitBinlogFileName(b)
	if !aOK || !bOK {
		return strings.Compare(a, b)
	}
	if c := strings.Compare(aBase, bBase); c != 0 {
		return c
	}
	if c := cmp.Compare(aSeq, bSeq); c != 0 {
		return c
	}
	// The same number written with other padding ("000010" and "10"): not a
	// name the server writes, but keep the order total and consistent with
	// equality.
	return strings.Compare(a, b)
}

// splitBinlogFileName splits "base.000123" into "base" and 123. ok is false
// when the name has no all-digit suffix after its last ".".
func splitBinlogFileName(name string) (base string, seq uint64, ok bool) {
	i := strings.LastIndexByte(name, '.')
	if i < 0 || i == len(name)-1 {
		return "", 0, false
	}
	seq, err := strconv.ParseUint(name[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return name[:i], seq, true
}

func (p ReplicationEventPosition) ToMysqlPos() mysql.Position {
	return mysql.Position{
		Name: p.Name,
		Pos:  p.Pos,
	}
}

func (p CdcPosition) ToSDKPosition() opencdc.Position {
	v, err := json.Marshal(Position{Version: CDCPositionVersion, CdcPosition: &p})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}
