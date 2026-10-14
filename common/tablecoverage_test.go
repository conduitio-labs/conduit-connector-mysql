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
			is.Equal(parsed.Version, CurrentPositionVersion)

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
