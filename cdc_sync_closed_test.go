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
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/matryer/is"
)

// TestIsSyncClosed guards the benign "Sync was closed" recognition. go-mysql
// returns ErrSyncClosed two ways depending on where canal.Close() interrupts
// RunFrom: as a proper wrapped error (errors.Is matches) later in the sync loop,
// and as a %v-formatted string ("start sync replication at binlog ... error Sync
// was closed") when Close lands during the initial start-sync phase (canal/
// sync.go). The latter breaks the Unwrap chain, so errors.Is alone misses it and
// Teardown/ReadN would wrongly report a fatal error — the exact CI failure this
// test locks against.
func TestIsSyncClosed(t *testing.T) {
	is := is.New(t)

	// The %v-formatted form go-mysql produces at start-sync (chain broken).
	startSyncFormatted := fmt.Errorf(
		"start sync replication at binlog (binlog.000002, 56244) error %v",
		replication.ErrSyncClosed)

	is.True(isSyncClosed(replication.ErrSyncClosed))                            // bare sentinel
	is.True(isSyncClosed(fmt.Errorf("wrapped: %w", replication.ErrSyncClosed))) // errors.Is form
	is.True(isSyncClosed(startSyncFormatted))                                   // %v string form
	is.True(!isSyncClosed(nil))                                                 // nil is not sync-closed
	is.True(!isSyncClosed(errors.New("some other canal failure")))              // unrelated error
}
