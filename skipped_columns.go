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
	"fmt"
	"sort"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
)

// CodeSkippedColumns is the stable error code carried by SkippedColumnsError.
// It joins the MYSQL_* family named in the type-semantics design
// (ConduitIO/conduit#3040, "Row images").
const CodeSkippedColumns = "MYSQL_ROW_IMAGE_SKIPPED_COLUMNS"

// ErrSkippedColumns matches (errors.Is) every SkippedColumnsError.
var ErrSkippedColumns = errors.New("row image skipped columns")

// SkippedColumnsError reports a row event whose image did not log every
// column. The server does that when the writing session (or the server) runs
// with binlog_row_image=MINIMAL or NOBLOB. The skipped values are not NULL,
// they are unknown, and go-mysql hands them over as nil.
type SkippedColumnsError struct {
	Table      string // schema.table
	Action     string // insert, update or delete
	BinlogFile string
	// StartPos and EndPos bound the rows event in BinlogFile. EndPos is the
	// log_pos MySQL prints in SHOW BINLOG EVENTS (end_log_pos).
	StartPos uint32
	EndPos   uint32
	// Columns are the distinct skipped column names, in table order.
	Columns []string
}

// Error returns the code, what happened, and the fix, on one line so it
// survives log pipelines and Conduit's error reporting.
func (e *SkippedColumnsError) Error() string {
	return fmt.Sprintf(
		"%s: %s on table %q at binlog file %q position %d-%d: the row image did not log column(s) [%s], "+
			"so their values are unknown and would be delivered as NULL "+
			"(config: binlog_row_image=MINIMAL or NOBLOB in the writing session or on the server). "+
			"Fix: set binlog_row_image=FULL for the writing session (SET SESSION binlog_row_image = 'FULL') and "+
			"for the server (SET GLOBAL binlog_row_image = 'FULL'). This event is already in the binlog and "+
			"will halt every restart: to find its transaction (and its GTID, if GTIDs are on) run "+
			"SHOW BINLOG EVENTS IN '%s' and read the events that precede position %d, "+
			"reconcile the affected rows downstream, then clear the connector's position to re-snapshot",
		CodeSkippedColumns, e.Action, e.Table, e.BinlogFile, e.StartPos, e.EndPos,
		strings.Join(e.Columns, ", "), e.BinlogFile, e.StartPos)
}

// Is makes errors.Is(err, ErrSkippedColumns) true.
func (e *SkippedColumnsError) Is(target error) bool { return target == ErrSkippedColumns }

// checkSkippedColumns returns a *SkippedColumnsError when any row image in the
// event left a column out, and nil otherwise. The caller fills BinlogFile,
// which needs the live canal position.
//
// Invariant 6: schema handling never silently mangles data. A nil in
// Rows[i][c] with c in SkippedColumns[i] means "not logged", not NULL;
// emitting it would turn an existing value into NULL downstream, so the
// whole event halts before any record from it is built.
//
// SkippedColumns is nil for rows read by the initial dump and holds an empty
// slice per row for a FULL image; neither halts.
func checkSkippedColumns(e *canal.RowsEvent) *SkippedColumnsError {
	seen := map[int]struct{}{}
	for _, cols := range e.SkippedColumns {
		for _, c := range cols {
			seen[c] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil
	}

	idx := make([]int, 0, len(seen))
	for c := range seen {
		idx = append(idx, c)
	}
	sort.Ints(idx)

	names := make([]string, 0, len(idx))
	for _, c := range idx {
		// The table schema can be newer than the event (DDL), so an index may
		// be out of range. Name it by position rather than guessing.
		if c >= 0 && c < len(e.Table.Columns) {
			names = append(names, e.Table.Columns[c].Name)
		} else {
			names = append(names, fmt.Sprintf("#%d", c))
		}
	}

	err := &SkippedColumnsError{
		Table:   e.Table.Schema + "." + e.Table.Name,
		Action:  e.Action,
		Columns: names,
	}
	if e.Header != nil {
		err.EndPos = e.Header.LogPos
		err.StartPos = e.Header.LogPos - e.Header.EventSize
	}
	return err
}
