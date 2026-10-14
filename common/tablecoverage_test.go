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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/matryer/is"
)

const goldenTablesHash = "1ebfea91daeea763f8201c152c25bac2051fef4690988c97eb0b342a33c32e8a"

func tableNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("customer_orders_%04d", i)
	}
	return out
}

func TestTableCoverage_ListVersusHash(t *testing.T) {
	is := is.New(t)

	listed := NewTableCoverage(tableNames(MaxListedTables), nil, nil)
	is.True(!listed.Hashed())
	is.Equal(len(listed.Snapshotted), MaxListedTables)

	hashed := NewTableCoverage(tableNames(MaxListedTables+1), nil, nil)
	is.True(hashed.Hashed())
	is.Equal(hashed.Count, MaxListedTables+1)
	is.Equal(len(hashed.Snapshotted), 0)

	// The union decides the form, not one set alone.
	mixed := NewTableCoverage(tableNames(150), tableNames(300)[150:], nil)
	is.True(mixed.Hashed())
}

func TestTableCoverage_AddedAndRestrict(t *testing.T) {
	is := is.New(t)

	c := NewTableCoverage([]string{"a", "b"}, []string{"u"}, []string{"c"})

	added, changed := c.Added([]string{"a", "b", "c", "u"})
	is.True(!changed)
	is.Equal(len(added), 0)

	added, changed = c.Added([]string{"a", "new", "u"})
	is.True(changed)
	is.Equal(added, []string{"new"})

	r := c.Restrict([]string{"a", "u", "z"})
	is.Equal(r.Snapshotted, []string{"a"})
	is.Equal(r.Unverified, []string{"u"})
	is.Equal(len(r.CDCOnly), 0)
}

func TestTableCoverage_HashDetectsChange(t *testing.T) {
	is := is.New(t)
	names := tableNames(MaxListedTables + 50)
	c := NewTableCoverage(names, nil, nil)

	_, changed := c.Added(names)
	is.True(!changed)
	_, changed = c.Added(append(append([]string{}, names...), "extra"))
	is.True(changed)
	_, changed = c.Added(names[1:]) // a table removed is indistinguishable from one added
	is.True(changed)
}

func TestTableCoverage_JSONRoundTripAndAdditiveCompat(t *testing.T) {
	for _, n := range []int{0, 3, MaxListedTables, MaxListedTables + 1} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			is := is.New(t)
			in := NewTableCoverage(tableNames(n), nil, nil)

			raw := CdcPosition{
				ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000001", Pos: 5},
				TableCoverage:            in,
			}.ToSDKPosition()

			parsed, err := ParseSDKPosition(raw)
			is.NoErr(err)
			is.Equal(*parsed.CdcPosition.TableCoverage, *in)
			is.Equal(parsed.Version, CDCPositionVersion)

			// A build that does not know the field still reads the position.
			var old oldPosition
			is.NoErr(json.Unmarshal(raw, &old))
			is.Equal(old.CdcPosition.Pos, uint32(5))
		})
	}
}

// Position size matters because the coverage rides on every CDC record.
func TestTableCoverage_PositionSize(t *testing.T) {
	for _, n := range []int{10, MaxListedTables, MaxListedTables + 1, 5000} {
		raw := CdcPosition{TableCoverage: NewTableCoverage(tableNames(n), nil, nil)}.ToSDKPosition()
		t.Logf("%5d tables: position is %6d bytes", n, len(raw))
		if len(raw) > 12*1024 {
			t.Fatalf("position for %d tables is %d bytes, over the 12 KiB budget", n, len(raw))
		}
	}
}

func BenchmarkCdcPosition_RoundTrip(b *testing.B) {
	for _, n := range []int{10, MaxListedTables, 1000, 5000} {
		pos := CdcPosition{
			ReplicationEventPosition: ReplicationEventPosition{Name: "binlog.000001", Pos: 5},
			TableCoverage:            NewTableCoverage(tableNames(n), nil, nil),
		}
		b.Run(fmt.Sprintf("tables=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var size int
			for range b.N {
				raw := pos.ToSDKPosition()
				size = len(raw)
				if _, err := ParseSDKPosition(raw); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size), "bytes/position")
		})
	}
}

// The hash is persisted in every CDC position and compared by a later build, so
// its encoding is a position format. This pins one value; if it changes, bump
// hashVersion and keep reading the old version rather than editing the value.
func TestTableCoverage_HashGolden(t *testing.T) {
	is := is.New(t)
	is.Equal(hashTables([]string{"a", "bc", "x\ny"}), goldenTablesHash)
}

// A hash written under a different version cannot be compared, so it must read
// as changed (never as covered): treating an unreadable hash as "unchanged"
// would be the same silent skip as the collision.
func TestTableCoverage_UnknownHashVersionReadsAsChanged(t *testing.T) {
	is := is.New(t)
	names := tableNames(MaxListedTables + 1)
	cov := NewTableCoverage(names, nil, nil)
	is.Equal(cov.HashVersion, hashVersion)

	_, changed := cov.Added(names)
	is.True(!changed)

	for _, v := range []int{0, hashVersion + 1} {
		stale := *cov
		stale.HashVersion = v
		_, changed := stale.Added(names)
		is.True(changed) // same table set, hash from another encoding
	}

	// A position written before the version tag existed has none.
	var legacy TableCoverage
	is.NoErr(json.Unmarshal([]byte(`{"hash":"`+cov.Hash+`","count":201}`), &legacy))
	_, changed = legacy.Added(names)
	is.True(changed)
}

// Re-baselining after a detected change must leave a readable coverage, so the
// warning is logged once and not on every start.
func TestTableCoverage_WithCDCOnlyRebaselinesHash(t *testing.T) {
	is := is.New(t)
	names := tableNames(MaxListedTables + 1)

	stale := NewTableCoverage(names, nil, nil)
	stale.HashVersion = 0
	_, changed := stale.Added(names)
	is.True(changed)

	re := stale.WithCDCOnly(nil, names)
	is.Equal(re.HashVersion, hashVersion)
	_, changed = re.Added(names)
	is.True(!changed)
}
