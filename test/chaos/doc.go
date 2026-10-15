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

// Package chaos kills the MySQL source with SIGKILL, as a real process, at
// chosen points and checks on recovery that nothing acked was lost or
// re-read and that positions never moved backwards (invariants 1 to 3 in
// Conduit's CLAUDE.md).
//
// # How it runs
//
// The test binary re-executes itself as a child process (TestMain routes on
// an environment variable). The child builds the connector's source with the
// production constructor (mysql.NewSource, SDK middleware included), opens it
// at the position the previous child stored, and loops:
//
//  1. ReadN a batch;
//  2. append every record to the ledger and fsync (the durable downstream);
//  3. Ack every record;
//  4. store the last record's position: append it to the position log, then
//     write the position file (temp file, fsync, rename, fsync the directory).
//
// That is the order the engine follows (persist downstream, ack, persist the
// position), so the ledger can hold a record whose position was never stored,
// but never the reverse. A child can park at a chosen point (after the Nth
// append, after the Nth ack, or after storing the position of a given row);
// the parent waits until it has parked and kills it with SIGKILL, so every
// kill lands exactly where the scenario says. One kill is timing-based.
//
// # What is checked
//
//   - No gap (invariant 3): every row written to the table is in the ledger.
//   - Exact resume: no record delivered after a restart is one whose position
//     was stored before the kill. Records that were appended or acked but whose
//     position was not stored may come again; they are counted, not failed.
//   - Monotonic positions (invariant 2): in the position log, the snapshot
//     phase never follows the CDC phase, the snapshot key never decreases, and
//     the CDC (file, offset, row) never decreases.
//
// # What it does not prove
//
// The child drives the source in process, without sdk.Serve and gRPC, and the
// ledger stands in for Conduit's destination and position persister. The
// engine's own ack and persist ordering is tested in ConduitIO/conduit
// (tests/chaos). Only MySQL 8.0 is exercised.
//
// Everything except this file is behind the "chaos" build tag:
//
//	MYSQL_TEST_DSN=... go test -race -count=1 -tags chaos ./test/chaos/
package chaos
