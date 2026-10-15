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
	"errors"
	"fmt"
)

// Error codes are public API: tooling and agents match on them. Never change
// or reuse a code; add a new one.
const (
	// CodeTablesUnverified: a position written by an older version could not say
	// whether the tables were snapshotted. Warning only.
	CodeTablesUnverified = "mysql.snapshot.tables_unverified"

	// CodeTablesAddedAfterCDC: tables were added to the connector's table list
	// after it entered CDC, and a snapshot would be needed to cover them.
	CodeTablesAddedAfterCDC = "mysql.snapshot.tables_added_after_cdc"

	// CodeUnsafeSnapshotRemoved: the removed snapshot.unsafe setting is true.
	CodeUnsafeSnapshotRemoved = "mysql.config.unsafe_snapshot_removed"

	// CodeTableWithoutKey: a snapshot is required for a table with neither a
	// primary key nor a sorting column.
	CodeTableWithoutKey = "mysql.snapshot.table_without_key"

	// CodeSortingColumnInvalid: a tableConfig sortingColumn is nullable or not
	// unique, so a snapshot ordered by it could skip rows.
	CodeSortingColumnInvalid = "mysql.snapshot.sorting_column_invalid"

	// CodeRowColumnsMismatch: a binlog row image has fewer values than the
	// table has columns now, so its values cannot be mapped to columns. The
	// table's schema changed after the event was written.
	CodeRowColumnsMismatch = "mysql.cdc.row_columns_mismatch"

	// CodeResumeEventNotFound: on a restart, the binlog event the saved
	// position names was not where the position says. Nothing after it is
	// delivered, because what was delivered before is unknown.
	CodeResumeEventNotFound = "mysql.cdc.resume_event_not_found"

	// CodeDeleteWithoutPrimaryKey: a delete record targets a table without a
	// primary key, so it cannot be matched to a row. The write fails instead of
	// deleting every row.
	CodeDeleteWithoutPrimaryKey = "mysql.destination.delete_without_primary_key"

	// CodeUnsupportedOperation: a record carries an operation the destination
	// does not apply. The write fails instead of dropping the record.
	CodeUnsupportedOperation = "mysql.destination.unsupported_operation"

	// CodeUnknownColumn: a record has a field the target table has no column
	// for. The write fails; the destination does not alter tables and does not
	// drop fields.
	CodeUnknownColumn = "mysql.destination.unknown_column"

	// CodePayloadNotStructured: a create, update or snapshot record does not
	// carry structured data in .Payload.After.
	CodePayloadNotStructured = "mysql.destination.payload_not_structured"

	// CodePayloadEmpty: a create, update or snapshot record has no fields.
	CodePayloadEmpty = "mysql.destination.payload_empty"

	// CodeKeyColumnMissing: a delete record's key does not hold a value for a
	// primary-key column of the table, or is not a JSON object.
	CodeKeyColumnMissing = "mysql.destination.key_column_missing"
)

// Error is a user-facing configuration or state error with a stable code, the
// configuration path at fault and a suggested fix. Match it with errors.Is
// against its sentinel, or errors.As to read the fields.
type Error struct {
	// Code is the stable, machine-readable error code.
	Code string
	// ConfigPath is the connector configuration parameter at fault, or
	// "position" when the persisted pipeline position is.
	ConfigPath string
	// Message says what is wrong, naming the offending values.
	Message string
	// Fix says what to do about it.
	Fix string

	sentinel error
}

func newError(sentinel error, code, configPath, fix, format string, args ...any) *Error {
	return &Error{
		Code:       code,
		ConfigPath: configPath,
		Message:    fmt.Sprintf(format, args...),
		Fix:        fix,
		sentinel:   sentinel,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s (config: %s). Fix: %s", e.Code, e.Message, e.ConfigPath, e.Fix)
}

// Is reports whether target is this error's sentinel.
func (e *Error) Is(target error) bool {
	return e.sentinel != nil && errors.Is(e.sentinel, target)
}

// ErrTablesAddedAfterCDC is the sentinel for CodeTablesAddedAfterCDC.
var ErrTablesAddedAfterCDC = errors.New("tables added after cdc")

// ErrUnsafeSnapshotRemoved is the sentinel for CodeUnsafeSnapshotRemoved.
var ErrUnsafeSnapshotRemoved = errors.New("snapshot.unsafe has been removed")

// ErrTableWithoutKey is the sentinel for CodeTableWithoutKey.
var ErrTableWithoutKey = errors.New("table has no primary key and no sorting column")

// ErrRowColumnsMismatch is the sentinel for CodeRowColumnsMismatch.
var ErrRowColumnsMismatch = errors.New("binlog row image does not match the table's columns")

// ErrResumeEventNotFound is the sentinel for CodeResumeEventNotFound.
var ErrResumeEventNotFound = errors.New("saved binlog event not found on resume")

// resumeFix is the suggested fix for CodeResumeEventNotFound.
const resumeFix = "this is a connector bug; please report it with the position from the log. To continue, reset the " +
	"pipeline's position, which snapshots every table again (expect duplicates)"

func errResumeEventNotFound(format string, args ...any) *Error {
	return newError(ErrResumeEventNotFound, CodeResumeEventNotFound, "position", resumeFix, format, args...)
}

// ErrDeleteWithoutPrimaryKey is the sentinel for CodeDeleteWithoutPrimaryKey.
var ErrDeleteWithoutPrimaryKey = errors.New("delete from a table without a primary key")

// ErrUnsupportedOperation is the sentinel for CodeUnsupportedOperation.
var ErrUnsupportedOperation = errors.New("unsupported operation")

// ErrUnknownColumn is the sentinel for CodeUnknownColumn.
var ErrUnknownColumn = errors.New("unknown column")

// ErrPayloadNotStructured is the sentinel for CodePayloadNotStructured.
var ErrPayloadNotStructured = errors.New("payload is not structured data")

// ErrPayloadEmpty is the sentinel for CodePayloadEmpty.
var ErrPayloadEmpty = errors.New("payload has no fields")

// ErrKeyColumnMissing is the sentinel for CodeKeyColumnMissing.
var ErrKeyColumnMissing = errors.New("record key is missing a primary-key column")
