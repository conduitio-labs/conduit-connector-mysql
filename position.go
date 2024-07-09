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
	"encoding/json"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type positionType string

const (
	positionTypeSnapshot positionType = "snapshot"
	positionTypeCDC      positionType = "cdc"
)

type position struct {
	Kind             positionType      `json:"kind"`
	SnapshotPosition *snapshotPosition `json:"snapshot_position"`
	CdcPosition      *cdcPosition      `json:"cdc_position"`
}

type snapshotPosition struct {
	Snapshots snapshotPositions `json:"snapshots,omitempty"`
}

func (p snapshotPosition) toSDKPosition() sdk.Position {
	v, err := json.Marshal(position{
		Kind:             positionTypeSnapshot,
		SnapshotPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

func (p snapshotPosition) Clone() snapshotPosition {
	var newPosition snapshotPosition
	newPosition.Snapshots = make(map[common.TableName]tablePosition)
	for k, v := range p.Snapshots {
		newPosition.Snapshots[k] = v
	}
	return newPosition
}

func parseSDKPosition(p sdk.Position) (position, error) {
	var pos position
	if err := json.Unmarshal(p, &pos); err != nil {
		return pos, fmt.Errorf("failed to parse position: %w", err)
	}
	return pos, nil
}

type snapshotPositions map[common.TableName]tablePosition

type tablePosition struct {
	LastRead    int `json:"last_read"`
	SnapshotEnd int `json:"snapshot_end"`
}

type cdcPosition struct {
	mysql.Position
}

func (p cdcPosition) toSDKPosition() sdk.Position {
	v, err := json.Marshal(position{
		Kind:        positionTypeCDC,
		CdcPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}
