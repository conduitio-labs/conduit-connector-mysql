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

	// CodeUnsignedOverflow: a BIGINT UNSIGNED value is above the largest Avro
	// long (2^63-1). The source stops instead of emitting a wrong value.
	CodeUnsignedOverflow = "mysql.types.unsigned_overflow"

	// CodeUnsignedOverflowLegacySnapshot: as CodeUnsignedOverflow, for a legacy
	// pipeline whose position is in the snapshot phase, where the type
	// configuration cannot be switched in place.
	CodeUnsignedOverflowLegacySnapshot = "mysql.types.unsigned_overflow_legacy_snapshot"

	// CodeNullInNotNull: a NULL arrived for a NOT NULL column in strict mode
	// (or for a key column). It is never emitted as a zero value.
	CodeNullInNotNull = "mysql.types.null_in_not_null"

	// CodeValueNotRepresentable: in strict mode a value does not fit its
	// column's schema type (an integer out of range, DECIMAL text that does not
	// parse as a double). It is never coerced.
	CodeValueNotRepresentable = "mysql.types.value_not_representable"

	// CodeTypesSwitchInSnapshot: the type configuration differs from the one
	// in a position that is still in the snapshot phase.
	CodeTypesSwitchInSnapshot = "mysql.types.switch_in_snapshot"

	// CodeTypesStampMismatch: the type configuration differs from the one in
	// a CDC-phase position, and types.acceptChangeFrom does not name it.
	CodeTypesStampMismatch = "mysql.types.stamp_mismatch"

	// CodeTypesInvalid: a types.* setting has a value this version does not
	// support, or the combination is invalid.
	CodeTypesInvalid = "mysql.config.types_invalid"

	// CodeNullabilityUnavailable: a captured column has no row in
	// information_schema.columns (permissions, or the table changed), so its
	// nullability is unknown in strict mode.
	CodeNullabilityUnavailable = "mysql.schema.nullability_unavailable"

	// CodeBinlogRowImage, CodeBinlogFormat, CodeBinlogRowValueOptions: a
	// server binlog setting under which row events do not carry full rows.
	// Refused in strict mode, a warning in legacy mode.
	CodeBinlogRowImage        = "mysql.config.binlog_row_image"
	CodeBinlogFormat          = "mysql.config.binlog_format"
	CodeBinlogRowValueOptions = "mysql.config.binlog_row_value_options"

	// CodeLegacyTypes: the source runs in legacy type mode. Warning only.
	CodeLegacyTypes = "mysql.types.legacy"
)

// Error is a user-facing configuration or state error with a stable code, the
// configuration path at fault and a suggested fix. Match it with errors.Is
// against its sentinel, or errors.As to read the fields.
type Error struct {
	// Code is the stable, machine-readable error code.
	Code string
	// ConfigPath is the connector configuration parameter at fault.
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

// ErrUnsignedOverflow is the sentinel for CodeUnsignedOverflow.
var ErrUnsignedOverflow = errors.New("unsigned value does not fit the column's schema type")

// ErrNullInNotNull is the sentinel for CodeNullInNotNull.
var ErrNullInNotNull = errors.New("null in a not null column")

// ErrValueNotRepresentable is the sentinel for CodeValueNotRepresentable.
var ErrValueNotRepresentable = errors.New("value does not fit the column's schema type")

// ErrTypesMismatch is the sentinel for CodeTypesSwitchInSnapshot and
// CodeTypesStampMismatch.
var ErrTypesMismatch = errors.New("type configuration differs from the position's")

// ErrTypesInvalid is the sentinel for CodeTypesInvalid.
var ErrTypesInvalid = errors.New("invalid types configuration")

// ErrNullabilityUnavailable is the sentinel for CodeNullabilityUnavailable.
var ErrNullabilityUnavailable = errors.New("column nullability unavailable")

// ErrBinlogSettings is the sentinel for the binlog setting codes.
var ErrBinlogSettings = errors.New("binlog settings do not log full rows")
