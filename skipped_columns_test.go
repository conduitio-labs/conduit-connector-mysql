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
				"binlog_row_image", "SET SESSION binlog_row_image = 'FULL'", "SHOW BINLOG EVENTS",
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
