package mysql

import (
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type positionType string

const (
	positionTypeSnapshot positionType = "snapshot"
	positionTypeCDC      positionType = "cdc"
)

type position struct {
	kind             positionType
	snapshotPosition *snapshotPosition
	cdcPosition      *cdcPosition
}

type snapshotPosition struct {
	Snapshots snapshotPositions `json:"snapshots,omitempty"`
}

func (p snapshotPosition) toSDKPosition() sdk.Position {
	v, err := json.Marshal(position{
		kind:             positionTypeSnapshot,
		snapshotPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}

func (p snapshotPosition) Clone() snapshotPosition {
	var newPosition snapshotPosition
	newPosition.Snapshots = make(map[tableName]tablePosition)
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

type snapshotPositions map[tableName]tablePosition

type tablePosition struct {
	LastRead    int `json:"last_read"`
	SnapshotEnd int `json:"snapshot_end"`
}

type cdcPosition struct {
	mysql.Position
}

func (p cdcPosition) toSDKPosition() sdk.Position {
	v, err := json.Marshal(position{
		kind:        positionTypeCDC,
		cdcPosition: &p,
	})
	if err != nil {
		// This should never happen, all Position structs should be valid.
		panic(err)
	}
	return v
}
