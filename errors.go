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
