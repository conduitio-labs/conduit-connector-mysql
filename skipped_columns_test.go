// Copyright © 2026 Meroxa, Inc.
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
	"errors"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/matryer/is"
)

func skippedTestTable() *schema.Table {
	return &schema.Table{
		Schema: "meroxadb",
		Name:   "t",
		Columns: []schema.TableColumn{
			{Name: "id"}, {Name: "name"}, {Name: "note"}, {Name: "doc"},
		},
	}
}

func TestCheckSkippedColumns(t *testing.T) {
	header := &replication.EventHeader{LogPos: 1456, EventSize: 222}

	tests := []struct {
		name        string
		skipped     [][]int
		wantErr     bool
		wantColumns []string
	}{
		{name: "dump rows have nil SkippedColumns", skipped: nil},
		{name: "FULL image has one empty entry per row", skipped: [][]int{{}, {}}},
		{
			name:        "MINIMAL update skips the unchanged columns in both images",
			skipped:     [][]int{{1, 2, 3}, {0, 2, 3}},
			wantErr:     true,
			wantColumns: []string{"id", "name", "note", "doc"},
		},
		{
			name:        "NOBLOB skips the BLOB only",
			skipped:     [][]int{{3}, {3}},
			wantErr:     true,
			wantColumns: []string{"doc"},
		},
		{
			name:        "one skipped row in a multi-row event halts the event",
			skipped:     [][]int{{}, {}, {2}},
			wantErr:     true,
			wantColumns: []string{"note"},
		},
		{
			name:        "index beyond the table schema is named by position",
			skipped:     [][]int{{9}},
			wantErr:     true,
			wantColumns: []string{"#9"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			e := &canal.RowsEvent{
				Table:          skippedTestTable(),
				Action:         canal.UpdateAction,
				Header:         header,
				SkippedColumns: tc.skipped,
			}

			err := checkSkippedColumns(e)
			if !tc.wantErr {
				is.True(err == nil)
				return
			}

			is.True(err != nil)
			is.Equal(err.Columns, tc.wantColumns)
			is.Equal(err.Table, "meroxadb.t")
			is.Equal(err.Action, canal.UpdateAction)
			is.Equal(err.EndPos, uint32(1456))
			is.Equal(err.StartPos, uint32(1234))

			err.BinlogFile = "mysql-bin.000003"
			var asErr error = err
			is.True(errors.Is(asErr, ErrSkippedColumns))
			msg := asErr.Error()
			for _, want := range []string{
				CodeSkippedColumns, `"meroxadb.t"`, "update", `"mysql-bin.000003"`, "1234-1456",
				"binlog_row_image", "SET SESSION binlog_row_image = 'FULL'", "SET PERSIST", "reconnect every writer",
				"does not carry deletes", "ALL captured tables", "SHOW BINLOG EVENTS",
				"truncate the destination and then re-snapshot", "after the snapshot completes, reconcile",
			} {
				is.True(strings.Contains(msg, want)) // message must contain want
			}
		})
	}
}

// A handler that sees a FULL image or a dump row must not halt, and must pass
// the rows on untouched.
func TestCdcEventHandler_OnRow_NoHaltWithoutSkippedColumns(t *testing.T) {
	for name, skipped := range map[string][][]int{
		"nil (dump)": nil,
		"FULL image": {{}},
	} {
		t.Run(name, func(t *testing.T) {
			is := is.New(t)
			recC := make(chan opencdc.Record, 1)
			called := false
			h := &cdcEventHandler{
				canalDoneC:     make(chan struct{}),
				parsedRecordsC: recC,
				onRowsChange: func(rowEvent) ([]opencdc.Record, error) {
					called = true
					return []opencdc.Record{{}}, nil
				},
			}

			err := h.OnRow(&canal.RowsEvent{
				Table:          skippedTestTable(),
				Action:         canal.InsertAction,
				Rows:           [][]any{{1, "a", nil, nil}},
				SkippedColumns: skipped,
			})
			is.NoErr(err)
			is.True(called)
			is.Equal(len(recC), 1)
		})
	}
}

// Sub-events of a compressed transaction carry LogPos 0. The start position
// must not underflow and the message must say the position is unknown.
func TestCheckSkippedColumns_CompressedTransaction(t *testing.T) {
	is := is.New(t)
	e := &canal.RowsEvent{
		Table:          skippedTestTable(),
		Action:         canal.UpdateAction,
		Header:         &replication.EventHeader{LogPos: 0, EventSize: 40},
		SkippedColumns: [][]int{{1}, {0}},
	}

	err := checkSkippedColumns(e)
	is.True(err != nil)
	is.True(err.Compressed)
	is.Equal(err.StartPos, uint32(0))
	is.Equal(err.EndPos, uint32(0))

	err.BinlogFile = "mysql-bin.000003"
	msg := err.Error()
	is.True(!strings.Contains(msg, "4294967"))
	is.True(strings.Contains(msg, "compressed transaction"))
	is.True(strings.Contains(msg, "exact event position unavailable"))
	is.True(strings.Contains(msg, "Transaction_payload"))
	is.True(!strings.Contains(msg, "FROM"))

	// EventSize larger than LogPos underflows the same way without LogPos == 0.
	e.Header = &replication.EventHeader{LogPos: 30, EventSize: 40}
	err = checkSkippedColumns(e)
	is.True(err != nil && err.Compressed && err.StartPos == 0)
}

func TestWrapCanalRunErr_PartialJSON(t *testing.T) {
	is := is.New(t)
	c := &cdcIterator{
		position: &common.CdcPosition{ReplicationEventPosition: common.ReplicationEventPosition{Name: "binlog.000002", Pos: 123}},
		// The form canal returns: a fresh error with the event type formatted in.
		canalRunErr: errors.New("PartialUpdateRowsEvent not supported now"),
	}

	err := c.wrapCanalRunErr()
	is.True(errors.Is(err, ErrPartialJSONUnsupported))
	is.True(!errors.Is(err, ErrCDCStartPositionUnavailable))
	is.True(strings.Contains(err.Error(), CodePartialJSONUnsupported))
	is.True(strings.Contains(err.Error(), "SET PERSIST binlog_row_value_options"))
	is.True(strings.Contains(err.Error(), "ALL captured tables"))
	is.True(strings.Contains(err.Error(), "re-snapshot and, after the snapshot completes, reconcile"))
	is.True(!strings.Contains(err.Error(), "after position 0")) // 0 is "unknown", not a position

	is.True(strings.Contains(err.Error(), "after position 123"))

	// A zero position (canal has saved none) is unknown, not a position.
	c.position.Pos = 0
	err = c.wrapCanalRunErr()
	is.True(errors.Is(err, ErrPartialJSONUnsupported))
	is.True(!strings.Contains(err.Error(), "after position"))
	is.True(strings.Contains(err.Error(), "exact event position unavailable"))

	// Only canal's full message counts, not any error that mentions the event name.
	is.True(isPartialJSON(errors.New("PartialUpdateRowsEvent not supported now")))
	is.True(!isPartialJSON(errors.New("saw a PartialUpdateRowsEvent")))

	// Any other canal failure is still reported as an unavailable start position.
	c.canalRunErr = errors.New("some other canal failure")
	is.True(errors.Is(c.wrapCanalRunErr(), ErrCDCStartPositionUnavailable))
}
