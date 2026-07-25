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
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

// TestSnapshotIterator_BuildRecord_StampsCDCStartOnFirstRecord is a pure unit
// test (no database) for the ordering property Invariant 3 depends on: P0 must
// be present on the FIRST emitted snapshot record, not just later ones, because
// the load-bearing property is "no record advances LastRead without also
// carrying cdc_start". See docs/design-documents/20260724-snapshot-cdc-position-handoff.md,
// "Critical ordering property".
func TestSnapshotIterator_BuildRecord_StampsCDCStartOnFirstRecord(t *testing.T) {
	is := is.New(t)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tablePrimaryKeys: common.TableKeys{"users": {"id"}},
		database:         "meroxadb",
		serverID:         "1",
	})
	is.NoErr(err)

	p0 := &common.CdcPosition{
		ReplicationEventPosition: common.ReplicationEventPosition{Name: "binlog.000001", Pos: 4},
	}
	iterator.setCDCStart(p0)

	data := fetchData{
		table:         "users",
		key:           opencdc.RawData("1"),
		payload:       opencdc.StructuredData{"id": 1},
		position:      common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: float64(1), SnapshotEnd: float64(1)}},
		payloadSchema: &schemaSubjectVersion{subject: "users_payload", version: 1},
		keySchema:     &schemaSubjectVersion{subject: "users_key", version: 1},
	}

	// This is the very first call to buildRecord - simulating the first record
	// of a fresh snapshot - and it must already carry cdc_start.
	rec := iterator.buildRecord(data)

	parsed, err := common.ParseSDKPosition(rec.Position)
	is.NoErr(err)
	is.True(parsed.SnapshotPosition != nil)
	is.True(parsed.SnapshotPosition.CDCStart != nil)
	is.Equal(*parsed.SnapshotPosition.CDCStart, *p0)

	// A second record for a different key must keep carrying it too.
	data2 := data
	data2.key = opencdc.RawData("2")
	data2.payload = opencdc.StructuredData{"id": 2}
	data2.position = common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: float64(2), SnapshotEnd: float64(2)}}

	rec2 := iterator.buildRecord(data2)
	parsed2, err := common.ParseSDKPosition(rec2.Position)
	is.NoErr(err)
	is.True(parsed2.SnapshotPosition.CDCStart != nil)
	is.Equal(*parsed2.SnapshotPosition.CDCStart, *p0)
}

// TestSnapshotIterator_BuildRecord_NoCDCStartWhenNotSet documents the
// unexercised (test-only) case: if setCDCStart is never called, buildRecord
// still functions (CDCStart just stays nil/omitted), which is the legacy
// behavior standalone snapshot-iterator tests rely on. Production callers always
// go through newCombinedIterator, which never skips setCDCStart for a non-empty
// snapshot (see the p0 == nil guard there).
func TestSnapshotIterator_BuildRecord_NoCDCStartWhenNotSet(t *testing.T) {
	is := is.New(t)

	iterator, err := newSnapshotIterator(snapshotIteratorConfig{
		tablePrimaryKeys: common.TableKeys{"users": {"id"}},
		database:         "meroxadb",
		serverID:         "1",
	})
	is.NoErr(err)

	data := fetchData{
		table:         "users",
		key:           opencdc.RawData("1"),
		payload:       opencdc.StructuredData{"id": 1},
		position:      common.TablePosition{SingleKey: &common.TablePositionSingleKey{LastRead: float64(1), SnapshotEnd: float64(1)}},
		payloadSchema: &schemaSubjectVersion{subject: "users_payload", version: 1},
		keySchema:     &schemaSubjectVersion{subject: "users_key", version: 1},
	}

	rec := iterator.buildRecord(data)

	parsed, err := common.ParseSDKPosition(rec.Position)
	is.NoErr(err)
	is.True(parsed.SnapshotPosition.CDCStart == nil)
}
