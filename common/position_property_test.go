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

package common

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"pgregory.net/rapid"
)

// Property tests for the position format (a serialized format: invariant 2)
// and for the table coverage it carries (invariant 3). The check count
// follows rapid: -rapid.checks or RAPID_CHECKS (default 100).

// genKey draws a snapshot key of any Go type the snapshot reader produces,
// with the value it must decode to: signed integers widen to int64, unsigned
// to uint64, everything else keeps its type.
//
// Floats are finite or infinite, never NaN: MySQL stores no NaN, and a NaN
// float64 key is refused on read (see decodeTagged). Times are within MySQL's
// DATETIME range, years 1000 to 9999, which RFC 3339 can represent.
func genKey(t *rapid.T, label string) (in, want any) {
	switch rapid.IntRange(0, 16).Draw(t, label+" type") {
	case 0:
		return nil, nil
	case 1:
		v := rapid.Int().Draw(t, label)
		return v, int64(v)
	case 2:
		v := rapid.Int8().Draw(t, label)
		return v, int64(v)
	case 3:
		v := rapid.Int16().Draw(t, label)
		return v, int64(v)
	case 4:
		v := rapid.Int32().Draw(t, label)
		return v, int64(v)
	case 5:
		v := rapid.Int64().Draw(t, label)
		return v, v
	case 6:
		v := rapid.Uint().Draw(t, label)
		return v, uint64(v)
	case 7:
		v := rapid.Uint8().Draw(t, label)
		return v, uint64(v)
	case 8:
		v := rapid.Uint32().Draw(t, label)
		return v, uint64(v)
	case 9:
		v := rapid.Uint64().Draw(t, label)
		return v, v
	case 10:
		v := rapid.OneOf(
			rapid.Float32Range(-math.MaxFloat32, math.MaxFloat32),
			rapid.SampledFrom([]float32{float32(math.Inf(1)), float32(math.Inf(-1))}),
		).Draw(t, label)
		return v, v
	case 11:
		v := rapid.OneOf(
			rapid.Float64Range(-math.MaxFloat64, math.MaxFloat64),
			rapid.SampledFrom([]float64{math.Inf(1), math.Inf(-1)}),
		).Draw(t, label)
		return v, v
	case 12:
		v := rapid.SliceOfN(rapid.Byte(), 0, 16).Draw(t, label)
		return v, bytes.Clone(v)
	case 13:
		v := rapid.String().Draw(t, label)
		return v, v
	case 14:
		v := genTime(t, label)
		return v, v
	case 15:
		v := rapid.Bool().Draw(t, label)
		return v, v
	default:
		v := rapid.Uint16().Draw(t, label)
		return v, uint64(v)
	}
}

// genTime draws a time whose wall clock is within MySQL's DATETIME range,
// in any fixed zone. The wall clock is what the driver returns for a DATETIME,
// so that is the range to cover; an instant in range can still have a wall
// clock in year 10000 in a positive zone.
func genTime(t *rapid.T, label string) time.Time {
	offset := rapid.IntRange(-14*60, 14*60).Draw(t, label+" zone minutes")
	return time.Date(
		rapid.IntRange(1000, 9999).Draw(t, label+" year"),
		time.Month(rapid.IntRange(1, 12).Draw(t, label+" month")),
		rapid.IntRange(1, 28).Draw(t, label+" day"),
		rapid.IntRange(0, 23).Draw(t, label+" hour"),
		rapid.IntRange(0, 59).Draw(t, label+" minute"),
		rapid.IntRange(0, 59).Draw(t, label+" second"),
		rapid.IntRange(0, 999_999_999).Draw(t, label+" nanosecond"),
		time.FixedZone("", offset*60),
	)
}

// keyCmp compares decoded keys: times by instant (the zone name is not
// written, only the offset), everything else exactly.
var keyCmp = cmp.Options{
	cmp.Comparer(func(a, b time.Time) bool { return a.Equal(b) }),
	cmpopts.EquateEmpty(),
}

func genSnapshotPosition(t *rapid.T) (in, want SnapshotPosition) {
	in.Snapshots, want.Snapshots = SnapshotPositions{}, SnapshotPositions{}
	tables := rapid.SliceOfNDistinct(rapid.String(), 0, 4, rapid.ID[string]).Draw(t, "tables")
	for _, table := range tables {
		keyType := rapid.SampledFrom([]string{"", "int", "varchar", "decimal"}).Draw(t, "key type")
		if rapid.Bool().Draw(t, "single key") {
			lr, lrWant := genKey(t, "last read")
			se, seWant := genKey(t, "snapshot end")
			in.Snapshots[table] = TablePosition{SingleKey: &TablePositionSingleKey{LastRead: lr, SnapshotEnd: se, KeyType: keyType}}
			want.Snapshots[table] = TablePosition{SingleKey: &TablePositionSingleKey{LastRead: lrWant, SnapshotEnd: seWant, KeyType: keyType}}
			continue
		}
		var inKeys, wantKeys TablePositionMultipleKey
		for i := range rapid.IntRange(1, 3).Draw(t, "key columns") {
			name := "k" + strconv.Itoa(i)
			lr, lrWant := genKey(t, "last read")
			se, seWant := genKey(t, "snapshot end")
			inKeys = append(inKeys, TablePositionMultipleKeyItem{KeyName: name, LastRead: lr, SnapshotEnd: se, KeyType: keyType})
			wantKeys = append(wantKeys, TablePositionMultipleKeyItem{KeyName: name, LastRead: lrWant, SnapshotEnd: seWant, KeyType: keyType})
		}
		in.Snapshots[table] = TablePosition{MultipleKey: &inKeys}
		want.Snapshots[table] = TablePosition{MultipleKey: &wantKeys}
	}
	if rapid.Bool().Draw(t, "cdc start") {
		cdc := genCdcPosition(t)
		in.CDCStart, want.CDCStart = &cdc, &cdc
	}
	return in, want
}

// genBinlogPos draws a binlog position: a file name with a six-digit sequence
// number, and an offset. MySQL pads the number to at least six digits and
// keeps counting past 999999 (seven digits and more); those names are left
// out because Compare orders them wrongly (ConduitIO/conduit-connector-mysql#238).
func genBinlogPos(t *rapid.T, label string) ReplicationEventPosition {
	return ReplicationEventPosition{
		Name: fmt.Sprintf("mysql-bin.%06d", rapid.IntRange(1, 999999).Draw(t, label+" file")),
		Pos:  rapid.Uint32().Draw(t, label+" offset"),
	}
}

func genCdcPosition(t *rapid.T) CdcPosition {
	p := CdcPosition{
		ReplicationEventPosition: genBinlogPos(t, "event"),
		Index:                    rapid.IntRange(0, 1<<20).Draw(t, "index"),
	}
	if rapid.Bool().Draw(t, "prev") {
		prev := genBinlogPos(t, "prev")
		p.PrevPosition = &prev
	}
	if rapid.Bool().Draw(t, "replay from") {
		replay := genBinlogPos(t, "replay from")
		p.ReplayFrom = &replay
	}
	if rapid.Bool().Draw(t, "compressed") {
		p.Compressed = &CompressedEventRef{
			Ordinal: rapid.IntRange(1, 1<<20).Draw(t, "ordinal"),
			Tables:  rapid.StringMatching(`[0-9a-f]{64}`).Draw(t, "tables fingerprint"),
		}
	}
	if rapid.Bool().Draw(t, "coverage") {
		p.TableCoverage, _ = genCoverage(t)
	}
	return p
}

// genCoverage draws a table coverage and the table set it covers. It comes
// in three forms:
//
//   - list form, from NewTableCoverage, for up to MaxListedTables tables;
//   - hash form, from NewTableCoverage, for more;
//   - legacy hash form, as builds before hashVersion 1 wrote it: no
//     HashVersion, and a SHA-256 of the sorted names joined with "\n". It is
//     built here because no current code writes it.
//
// Names may contain any character, newline included, which is what makes
// the legacy hash ambiguous.
func genCoverage(t *rapid.T) (*TableCoverage, []string) {
	if rapid.IntRange(0, 1).Draw(t, "legacy hash form") == 0 {
		set := sortedSet(genTableSet(t, "legacy covered"))
		sum := sha256.Sum256([]byte(strings.Join(set, "\n")))
		return &TableCoverage{
			Hash:          hex.EncodeToString(sum[:]),
			Count:         len(set),
			HasUnverified: rapid.Bool().Draw(t, "has unverified"),
		}, set
	}
	union := genTableSet(t, "covered")
	var snap, unver, cdcOnly []string
	for _, table := range union {
		switch rapid.IntRange(0, 2).Draw(t, "coverage kind") {
		case 0:
			snap = append(snap, table)
		case 1:
			unver = append(unver, table)
		default:
			cdcOnly = append(cdcOnly, table)
		}
	}
	return NewTableCoverage(snap, unver, cdcOnly), sortedSet(union)
}

// genTableSet draws distinct table names, sometimes more than
// MaxListedTables so the coverage takes its hash form.
func genTableSet(t *rapid.T, label string) []string {
	if rapid.IntRange(0, 4).Draw(t, label+" large") == 0 {
		n := rapid.IntRange(MaxListedTables+1, MaxListedTables+20).Draw(t, label+" count")
		set := make([]string, n)
		for i := range set {
			set[i] = "t" + strconv.Itoa(i)
		}
		return set
	}
	return rapid.SliceOfNDistinct(tableName, 0, 8, rapid.ID[string]).Draw(t, label)
}

// tableName draws a table name: often one of a few fixed names, some with a
// newline (the separator the legacy coverage hash joined names with, see
// legacyHashMatches), otherwise any string. The fixed names make a newline
// name, and sets that share names, common enough that 100 checks exercise
// them; rapid.String alone produces a newline in about 1 case in 1000.
var tableName = rapid.OneOf(
	rapid.SampledFrom([]string{"a", "b", "a\nb", "orders\n", "\n"}),
	rapid.SampledFrom([]string{"a\nb", "orders\n", "\n"}),
	rapid.String(),
)

func parseCDC(t *rapid.T, raw []byte) CdcPosition {
	pos, err := ParseSDKPosition(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
	if pos.CdcPosition == nil || pos.SnapshotPosition != nil {
		t.Fatalf("not a CDC position: %s", raw)
	}
	return *pos.CdcPosition
}

// TestSnapshotPosition_PropertyRoundTrip: every snapshot position reads back
// with each key at its exact value and type (keys of other integer widths
// widened), so a resumed snapshot seeks where it stopped.
func TestSnapshotPosition_PropertyRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		in, want := genSnapshotPosition(t)
		raw, err := in.ToSDKPosition()
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		pos, err := ParseSDKPosition(raw)
		if err != nil {
			t.Fatalf("parse %s: %v", raw, err)
		}
		if pos.Version != SnapshotPositionVersion || pos.SnapshotPosition == nil || pos.CdcPosition != nil {
			t.Fatalf("wrong envelope: %s", raw)
		}
		if diff := cmp.Diff(want, *pos.SnapshotPosition, keyCmp); diff != "" {
			t.Fatalf("snapshot position changed in a round trip (-want +got):\n%s\n%s", diff, raw)
		}
	})
}

// TestCdcPosition_PropertyRoundTrip: every CDC position reads back as
// written, and writing it again gives the same bytes.
func TestCdcPosition_PropertyRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		p := genCdcPosition(t)
		raw := p.ToSDKPosition()
		got := parseCDC(t, raw)
		if diff := cmp.Diff(p, got, cmpopts.EquateEmpty()); diff != "" {
			t.Fatalf("CDC position changed in a round trip (-want +got):\n%s\n%s", diff, raw)
		}
		if again := got.ToSDKPosition(); !bytes.Equal(raw, again) {
			t.Fatalf("rewrite changed the bytes:\nfirst:  %s\nsecond: %s", raw, again)
		}
	})
}

// TestCdcPosition_PropertyOrderSurvivesSerialization: for binlog file names
// with six-digit sequence numbers (mysql-bin.000001 to mysql-bin.999999),
// Compare orders positions as the binlog does (file sequence, then offset),
// is antisymmetric, and gives the same answer after a round trip. The CDC
// iterator compares these to decide what a restart skips (invariant 3).
//
// It does not cover names past .999999: MySQL keeps counting with seven or
// more digits, and Compare, which compares names as strings, orders those
// wrongly. That is ConduitIO/conduit-connector-mysql#238; widen the range to
// 1<<31-1 when it is fixed.
func TestCdcPosition_PropertyOrderSurvivesSerialization(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		aSeq := rapid.IntRange(1, 999999).Draw(t, "a file")
		bSeq := rapid.IntRange(1, 999999).Draw(t, "b file")
		aOff := rapid.Uint32().Draw(t, "a offset")
		bOff := rapid.Uint32().Draw(t, "b offset")
		a := CdcPosition{ReplicationEventPosition: ReplicationEventPosition{Name: fmt.Sprintf("mysql-bin.%06d", aSeq), Pos: aOff}}
		b := CdcPosition{ReplicationEventPosition: ReplicationEventPosition{Name: fmt.Sprintf("mysql-bin.%06d", bSeq), Pos: bOff}}

		var want int
		switch {
		case aSeq != bSeq:
			want = cmpInt(aSeq, bSeq)
		default:
			want = cmpInt(aOff, bOff)
		}
		if got := a.Compare(b.ReplicationEventPosition); got != want {
			t.Fatalf("Compare(%v, %v) = %d, want %d", a.ReplicationEventPosition, b.ReplicationEventPosition, got, want)
		}
		ra := parseCDC(t, a.ToSDKPosition())
		rb := parseCDC(t, b.ToSDKPosition())
		if got := ra.Compare(rb.ReplicationEventPosition); got != want {
			t.Fatalf("after a round trip, Compare = %d, want %d", got, want)
		}
		if got := b.Compare(a.ReplicationEventPosition); got != -want {
			t.Fatalf("Compare is not antisymmetric: %d and %d", want, got)
		}
	})
}

func cmpInt[T int | uint32](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// TestPosition_PropertyVersionGate: a position of any version up to
// CurrentPositionVersion is read; any higher version is refused with
// ErrPositionVersionUnsupported, never misread.
func TestPosition_PropertyVersionGate(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(genCdcPosition(t).ToSDKPosition(), &obj); err != nil {
			t.Fatal(err)
		}
		v := rapid.IntRange(0, CurrentPositionVersion+100).Draw(t, "version")
		obj["version"] = json.RawMessage(strconv.Itoa(v))
		raw, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ParseSDKPosition(raw)
		switch {
		case v <= CurrentPositionVersion && err != nil:
			t.Fatalf("version %d refused: %v", v, err)
		case v > CurrentPositionVersion && !errors.Is(err, ErrPositionVersionUnsupported):
			t.Fatalf("version %d: got %v, want ErrPositionVersionUnsupported", v, err)
		}
	})
}

// TestTableCoverage_PropertyNoMissedTable: after the coverage is written
// into a CDC position and read back, it never reports "nothing added" for a
// configuration that has a table it did not cover. A missed table would have
// its existing rows silently skipped (invariant 3).
//
//   - List form: the added tables are exactly the new ones.
//   - Hash form: any change to the set is reported.
//   - Legacy hash form: "unchanged" only when the set is the same and no name
//     contains a newline (the only case the legacy hash can prove);
//     otherwise changed, the safe answer.
func TestTableCoverage_PropertyNoMissedTable(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		cov, covered := genCoverage(t)
		p := genCdcPosition(t)
		p.TableCoverage = cov
		read := parseCDC(t, p.ToSDKPosition()).TableCoverage
		if read == nil {
			t.Fatal("coverage lost in a round trip")
		}
		if diff := cmp.Diff(cov, read, cmpopts.EquateEmpty()); diff != "" {
			t.Fatalf("coverage changed in a round trip (-want +got):\n%s", diff)
		}

		current := genCurrent(t, covered)
		added, changed := read.Added(current)

		switch {
		case read.Hashed() && read.HashVersion == 0:
			provable := sameSet(current, covered) && !slices.ContainsFunc(current, func(s string) bool {
				return strings.Contains(s, "\n")
			})
			if changed != !provable || added != nil {
				t.Fatalf("legacy hash form: Added = %q, %v; want changed = %v (current %q, covered %q)",
					added, changed, !provable, current, covered)
			}
		case read.Hashed():
			if changed != !sameSet(current, covered) || added != nil {
				t.Fatalf("hash form: Added = %q, %v for current %d tables, covered %d", added, changed, len(current), read.Count)
			}
		default:
			var want []string
			for _, table := range sortedSet(current) {
				if !slices.Contains(covered, table) {
					want = append(want, table)
				}
			}
			if diff := cmp.Diff(want, added, cmpopts.EquateEmpty()); diff != "" || changed != (len(want) > 0) {
				t.Fatalf("list form: Added(%q) = %q, %v; want %q (-want +got):\n%s", current, added, changed, want, diff)
			}
		}
	})
}

// genCurrent draws the table set a restart runs with: the covered set with
// some tables removed and some added.
func genCurrent(t *rapid.T, covered []string) []string {
	var current []string
	for _, table := range covered {
		if rapid.IntRange(0, 9).Draw(t, "drop table") != 0 {
			current = append(current, table)
		}
	}
	return append(current, rapid.SliceOfN(tableName, 0, 3).Draw(t, "new tables")...)
}

func sameSet(a, b []string) bool {
	return slices.Equal(sortedSet(a), sortedSet(b))
}
