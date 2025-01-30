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
	Type             PositionType      `json:"type"`
	SnapshotPosition *SnapshotPosition `json:"snapshot_single_key_position,omitempty"`
	CdcPosition      *CdcPosition      `json:"cdc_position,omitempty"`
}

type SnapshotPosition struct {
	Snapshots TablePositions `json:"snapshots,omitempty"`
}

func (p SnapshotPosition) ToSDKPosition() opencdc.Position {
	v, err := json.Marshal(Position{
		Type:             PositionTypeSnapshot,
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
	newPosition.Snapshots = make(TablePositions)
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

// TablePositions represents the current snapshot status of every table
// that has been snapshotted.
type TablePositions map[string]TablePosition

type TablePositionType string

const (
	TablePositionSingleKey   TablePositionType = "single_key"
	TablePositionMultipleKey TablePositionType = "multiple_keys"
)

type TablePosition struct {
	Type         TablePositionType    `json:"type"`
	SingleKey    *SingleKeyPosition   `json:"single_key_position"`
	MultipleKeys *MultipleKeyPosition `json:"multiple_keys_position"`
}

type SingleKeyPosition struct {
	LastRead    any `json:"last_read"`
	SnapshotEnd any `json:"snapshot_end"`
}

type MultipleKeyPosition []MultipleKeySinglePosition

type MultipleKeySinglePosition struct {
	KeyName     string `json:"key_name"`
	LastRead    any    `json:"last_read"`
	SnapshotEnd any    `json:"snapshot_end"`
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
		Type:        PositionTypeCDC,
		CdcPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}
