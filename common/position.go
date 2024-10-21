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
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type PositionType string

const (
	PositionTypeSnapshot PositionType = "snapshot"
	PositionTypeCDC      PositionType = "cdc"
)

type Position struct {
	Kind             PositionType      `json:"kind"`
	SnapshotPosition *SnapshotPosition `json:"snapshot_position,omitempty"`
	CdcPosition      *CdcPosition      `json:"cdc_position,omitempty"`
}

type SnapshotPosition struct {
	Snapshots SnapshotPositions `json:"snapshots,omitempty"`
}

func (p SnapshotPosition) ToSDKPosition() opencdc.Position {
	v, err := json.Marshal(Position{
		Kind:             PositionTypeSnapshot,
		SnapshotPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

func (p SnapshotPosition) Clone() SnapshotPosition {
	var newPosition SnapshotPosition
	newPosition.Snapshots = make(SnapshotPositions)
	for k, v := range p.Snapshots {
		newPosition.Snapshots[k] = v
	}
	return newPosition
}

func ParseSDKPosition(p opencdc.Position) (Position, error) {
	var pos Position
	if err := json.Unmarshal(p, &pos); err != nil {
		return pos, fmt.Errorf("failed to parse position: %w", err)
	}
	return pos, nil
}

// SnapshotPositions represents the current snapshot status of every table
// that has been snapshotted.
type SnapshotPositions map[string]TablePosition

type TablePosition struct {
	LastRead    int64 `json:"last_read"`
	SnapshotEnd int64 `json:"snapshot_end"`
}

type CdcPosition struct {
	// Name represents the mysql binlog filename.
	Name string `json:"name"`
	Pos  uint32 `json:"pos"`
}

func (p CdcPosition) ToMysqlPos() mysql.Position {
	return mysql.Position{
		Name: p.Name,
		Pos:  p.Pos,
	}
}

func (p CdcPosition) ToSDKPosition() opencdc.Position {
	v, err := json.Marshal(Position{
		Kind:        PositionTypeCDC,
		CdcPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

// TableKeys is a collection of associations between table names and primary key
// names. Useful for retrieving primary key values from rows.
type TableKeys map[string]string
