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
// It follows the mysql.<area>.<reason> style of the other codes in errors.go.
const CodeSkippedColumns = "mysql.cdc.row_image_skipped_columns"

// ErrSkippedColumns matches (errors.Is) every SkippedColumnsError.
var ErrSkippedColumns = errors.New("row image skipped columns")

// recoveryNote is the part of every halt message that states what resuming
// costs. A re-snapshot does not carry deletes, so it comes first.
const recoveryNote = "Recovery re-snapshots, and a snapshot does not carry deletes: either truncate the destination " +
	"and then re-snapshot, or re-snapshot and, after the snapshot completes, reconcile the destination including " +
	"deletes (reconciling before the snapshot misses rows deleted in between), for ALL captured tables"

// SkippedColumnsError reports a row event whose image did not log every
// column. The server does that when the writing session (or the server) runs
// with binlog_row_image=MINIMAL or NOBLOB, for updates and deletes and also
// for inserts (every column the INSERT does not name is skipped, defaulted and
// nullable ones included). The skipped values are unknown: not NULL and not
// the column default. go-mysql hands them over as nil.
type SkippedColumnsError struct {
	Table      string // schema.table
	Action     string // insert, update or delete
	BinlogFile string
	// StartPos and EndPos bound the rows event in BinlogFile. EndPos is the
	// log_pos MySQL prints in SHOW BINLOG EVENTS (end_log_pos).
	// Both are zero when Compressed is true.
	StartPos uint32
	EndPos   uint32
	// Compressed is true when the event came out of a compressed transaction
	// (binlog_transaction_compression=ON). go-mysql gives such sub-events a
	// zero log position, so the exact position is unknown. canal's saved
	// position is 0 in that case too, so there is nothing better to offer.
	Compressed bool
	// Columns are the distinct skipped column names, in table order.
	Columns []string
}

// Error returns the code, what happened, and the fix, on one line so it
// survives log pipelines and Conduit's error reporting.
func (e *SkippedColumnsError) Error() string {
	var where, find string
	if e.Compressed {
		where = "inside a compressed transaction (binlog_transaction_compression=ON), exact event position unavailable"
		find = fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' and look for the Transaction_payload events", e.BinlogFile)
	} else {
		where = fmt.Sprintf("position %d-%d", e.StartPos, e.EndPos)
		find = fmt.Sprintf("SHOW BINLOG EVENTS IN '%s' and read the events that precede position %d", e.BinlogFile, e.StartPos)
	}
	return fmt.Sprintf(
		"%s: %s on table %q at binlog file %q %s: the row image did not log column(s) [%s], "+
			"so their values are unknown (delivering the event would overwrite real data with NULL or zero values; "+
			"for an INSERT the true value is the column default) "+
			"(config: binlog_row_image=MINIMAL or NOBLOB in the writing session or on the server). "+
			"Fix: set binlog_row_image=FULL for the writers (SET SESSION binlog_row_image = 'FULL', and for the "+
			"server SET PERSIST binlog_row_image = 'FULL'; SET GLOBAL alone does not survive a restart) and "+
			"reconnect every writer, because existing sessions, pooled ones included, keep their value. "+
			"This event is already in the binlog and will halt every restart. %s. "+
			"To find its transaction (and its GTID, if GTIDs are on) run %s, "+
			"then clear the connector's position to re-snapshot",
		CodeSkippedColumns, e.Action, e.Table, e.BinlogFile, where, strings.Join(e.Columns, ", "),
		recoveryNote, find)
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
		if e.Header.LogPos == 0 || e.Header.EventSize > e.Header.LogPos {
			// Sub-event of a compressed transaction: LogPos is 0, and
			// LogPos-EventSize would underflow. Report it as unknown.
			err.Compressed = true
		} else {
			err.EndPos = e.Header.LogPos
			err.StartPos = e.Header.LogPos - e.Header.EventSize
		}
	}
	return err
}

// CodePartialJSONUnsupported is the stable error code for ErrPartialJSONUnsupported.
const CodePartialJSONUnsupported = "mysql.cdc.partial_json_unsupported"

// ErrPartialJSONUnsupported matches (errors.Is) every PartialJSONError.
var ErrPartialJSONUnsupported = errors.New("partial JSON update unsupported")

// partialUpdateEventName is how go-mysql names a PartialUpdateRowsEvent in the
// error canal returns for event types it does not handle ("<type> not
// supported now"). canal formats the event type into a fresh error, so only a
// string match recognizes it.
const partialUpdateEventName = "PartialUpdateRowsEvent not supported"

// PartialJSONError reports that canal stopped on a PartialUpdateRowsEvent,
// which a writer produces with binlog_row_value_options=PARTIAL_JSON when it
// updates part of a JSON column. canal rejects it before the handler runs, so
// the row cannot be read at all.
type PartialJSONError struct {
	BinlogFile string
	// AfterPos is the last position canal saved before the event.
	AfterPos uint32
	Cause    error
}

func (e *PartialJSONError) Error() string {
	// canal's saved position is 0 until it has saved one (always, inside
	// compressed transactions); do not print it as if it were real.
	where := fmt.Sprintf("binlog file %q, exact event position unavailable", e.BinlogFile)
	if e.AfterPos > 0 {
		where = fmt.Sprintf("binlog file %q, after position %d", e.BinlogFile, e.AfterPos)
	}
	return fmt.Sprintf(
		"%s: %s: the server logged a partial JSON update "+
			"(config: binlog_row_value_options=PARTIAL_JSON), which this connector cannot read: %v. "+
			"Fix: disable it for the writers (SET SESSION binlog_row_value_options = '', and for the server "+
			"SET PERSIST binlog_row_value_options = '') and reconnect every writer, because existing sessions keep their "+
			"value. The event is already in the binlog and will fail every restart. %s. "+
			"Then clear the connector's position to re-snapshot",
		CodePartialJSONUnsupported, where, e.Cause, recoveryNote)
}

// Unwrap returns the canal error.
func (e *PartialJSONError) Unwrap() error { return e.Cause }

// Is makes errors.Is(err, ErrPartialJSONUnsupported) true.
func (e *PartialJSONError) Is(target error) bool { return target == ErrPartialJSONUnsupported }

// isPartialJSON reports whether err is canal's rejection of a
// PartialUpdateRowsEvent.
func isPartialJSON(err error) bool {
	return err != nil && strings.Contains(err.Error(), partialUpdateEventName)
}
