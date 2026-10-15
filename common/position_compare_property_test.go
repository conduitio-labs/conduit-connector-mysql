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

package common

import (
	"cmp"
	"fmt"
	"math"
	"testing"

	"pgregory.net/rapid"
)

// genBinlogName draws a binlog file name: mostly the names the server
// writes (a base name, a dot, a sequence number padded to at least six
// digits, up to 2^31-1), and also names it does not write but a stored
// position could hold: other padding, no suffix, a non-numeric suffix, a
// number too large for uint64, or arbitrary text.
func genBinlogName(t *rapid.T, label string) string {
	base := rapid.SampledFrom([]string{"binlog", "mysql-bin", "a", "a.b"}).Draw(t, label+" base")
	seq := rapid.OneOf(
		rapid.Uint64Range(1, 20),
		rapid.Uint64Range(999_990, 1_000_010),
		rapid.Uint64Range(1, math.MaxInt32),
	).Draw(t, label+" seq")
	switch rapid.IntRange(0, 9).Draw(t, label+" kind") {
	case 9:
		// Names around the six-digit boundary, where text order and number
		// order disagree, mixed with unnumbered names that sort between them
		// as text. Drawn often so 100 checks meet a mixed triple.
		return rapid.SampledFrom(boundaryNames).Draw(t, label+" boundary")
	case 0:
		return fmt.Sprintf("%s.%d", base, seq) // other padding
	case 1:
		return base // no suffix
	case 2, 8:
		return base + "." + rapid.StringMatching(`[0-9]{0,3}[a-z][0-9a-z]{0,3}`).Draw(t, label+" suffix")
	case 3:
		return base + ".99999999999999999999999" // past uint64
	case 4:
		return rapid.String().Draw(t, label+" text")
	default:
		return fmt.Sprintf("%s.%06d", base, seq) // as the server writes it
	}
}

// boundaryNames are names around the six-digit boundary, where text order
// and number order disagree, mixed with unnumbered names that sort between
// them as text.
var boundaryNames = []string{
	"binlog.999999", "binlog.1000000", "binlog.000001", "binlog.1x", "binlog.00000x",
	"binlog.", "binlog", "binlog.1000000x", "a.b.000001", "",
}

// TestCompare_TransitiveOverBoundaryNames checks every triple of
// boundaryNames, so the mixed cases the property meets only by chance are
// always covered. Ordering unnumbered names as text against numbered ones
// broke transitivity: "binlog.1000000" < "binlog.1x" < "binlog.999999" as
// text, but "binlog.999999" < "binlog.1000000" by number.
func TestCompare_TransitiveOverBoundaryNames(t *testing.T) {
	pos := func(n string) ReplicationEventPosition { return ReplicationEventPosition{Name: n, Pos: 4} }
	for _, a := range boundaryNames {
		for _, b := range boundaryNames {
			for _, c := range boundaryNames {
				pa, pb, pc := pos(a), pos(b), pos(c)
				if pa.Compare(pb) <= 0 && pb.Compare(pc) <= 0 && pa.Compare(pc) > 0 {
					t.Fatalf("not transitive: %q <= %q <= %q but %q > %q", a, b, c, a, c)
				}
				if pa.Compare(pb) != -pb.Compare(pa) {
					t.Fatalf("not antisymmetric: %q and %q", a, b)
				}
			}
		}
	}
}

// triplePool are three names whose order is a cycle when unnumbered names
// compare to numbered ones as text: 999999 < 1000000 by number, but
// "1000000" < "1x" < "999999" as text.
var triplePool = []string{"binlog.999999", "binlog.1000000", "binlog.1x"}

func genEventPosition(t *rapid.T, label string) ReplicationEventPosition {
	return ReplicationEventPosition{
		Name: genBinlogName(t, label),
		Pos:  rapid.SampledFrom([]uint32{4, 900, math.MaxUint32}).Draw(t, label+" offset"),
	}
}

// TestCompare_PropertyTotalOrder: over any mix of names a stored position can
// hold, Compare is a total order: antisymmetric, transitive, and 0 exactly
// for equal positions. A resume compares the saved position with every
// event it reads, so an order that is not transitive could keep or drop an
// event depending on what came before it (invariant 3).
//
// A transitivity bug shows only on a particular triple, which independent
// draws of arbitrary names almost never produce. So half the cases draw all
// three positions from triplePool, the names where number order and text
// order disagree; with #239's order this fails within 100 checks. The other
// half draw from genEventPosition. TestCompare_TransitiveOverBoundaryNames
// checks every triple of the boundary names exhaustively and stays the
// guard of record.
func TestCompare_PropertyTotalOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		gen := rapid.Custom(func(t *rapid.T) ReplicationEventPosition { return genEventPosition(t, "") })
		if rapid.Bool().Draw(t, "from the boundary pool") {
			gen = rapid.Custom(func(t *rapid.T) ReplicationEventPosition {
				return ReplicationEventPosition{Name: rapid.SampledFrom(triplePool).Draw(t, "name"), Pos: 4}
			})
		}
		a, b, c := gen.Draw(t, "a"), gen.Draw(t, "b"), gen.Draw(t, "c")
		ab, ba, bc, ac := a.Compare(b), b.Compare(a), b.Compare(c), a.Compare(c)
		if ab != -ba {
			t.Fatalf("not antisymmetric: Compare(%v, %v) = %d, reverse %d", a, b, ab, ba)
		}
		if (ab == 0) != (a == b) {
			t.Fatalf("Compare(%v, %v) = %d, but equal = %v", a, b, ab, a == b)
		}
		if ab <= 0 && bc <= 0 && ac > 0 {
			t.Fatalf("not transitive: %v <= %v <= %v but Compare(a, c) = %d", a, b, c, ac)
		}
	})
}

// TestCompare_PropertyServerNamesInNumberOrder: for names the server writes
// with one base name, Compare follows the sequence number, then the offset.
func TestCompare_PropertyServerNamesInNumberOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		base := rapid.SampledFrom([]string{"binlog", "mysql-bin", "a.b"}).Draw(t, "base")
		seq := rapid.Uint64Range(1, math.MaxInt32)
		aSeq, bSeq := seq.Draw(t, "a seq"), seq.Draw(t, "b seq")
		aPos, bPos := rapid.Uint32().Draw(t, "a offset"), rapid.Uint32().Draw(t, "b offset")
		a := ReplicationEventPosition{Name: fmt.Sprintf("%s.%06d", base, aSeq), Pos: aPos}
		b := ReplicationEventPosition{Name: fmt.Sprintf("%s.%06d", base, bSeq), Pos: bPos}

		want := cmp.Compare(aPos, bPos)
		if aSeq != bSeq {
			want = cmp.Compare(aSeq, bSeq)
		}
		if got := a.Compare(b); got != want {
			t.Fatalf("Compare(%v, %v) = %d, want %d", a, b, got, want)
		}
	})
}

// TestCompare_PropertyUnnumberedFirst: a name without a numeric suffix sorts
// before every name with one.
func TestCompare_PropertyUnnumberedFirst(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := ReplicationEventPosition{Name: genBinlogName(t, "a"), Pos: 4}
		b := ReplicationEventPosition{Name: fmt.Sprintf("binlog.%06d", rapid.Uint64Range(1, math.MaxInt32).Draw(t, "seq")), Pos: 4}
		if _, _, numbered := splitBinlogFileName(a.Name); numbered {
			return
		}
		if got := a.Compare(b); got != -1 {
			t.Fatalf("Compare(%q, %q) = %d, want -1", a.Name, b.Name, got)
		}
	})
}
