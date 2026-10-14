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
	"fmt"
	"slices"
	"testing"

	"github.com/matryer/is"
)

// fillerNames returns n names that sort before "x".
func fillerNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("filler_%04d", i)
	}
	return out
}

// A MySQL table name may contain a newline, so a hash input that joins names
// with one is ambiguous: the tables "x" and "y" and the single table "x\ny"
// produced the same hash, and Added reported no change when the second set
// replaced the first. The new table's existing rows were then never
// snapshotted. Invariant 3.
//
// Only the first two cases are regressions: they fail on the newline-join
// encoding. The remaining three ("length prefix lookalike", "empty name",
// "nul byte") also pass on the old encoding; they are encoding-property checks
// that guard the length-prefixed form against its own likely mistakes.
func TestTableCoverage_HashIsInjective(t *testing.T) {
	cases := []struct {
		name    string
		covered []string
		current []string
	}{
		{"newline in a name", []string{"x", "y"}, []string{"x\ny"}},
		{"newline moved", []string{"a\nb", "c"}, []string{"a", "b\nc"}},
		{"length prefix lookalike", []string{"ab"}, []string{"a", "b"}},
		{"empty name", []string{"a"}, []string{"", "a"}},
		{"nul byte", []string{"x", "y"}, []string{"x\x00y"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			filler := fillerNames(MaxListedTables)
			cov := NewTableCoverage(append(slices.Clone(filler), tc.covered...), nil, nil)
			is.True(cov.Hashed())

			_, changed := cov.Added(append(slices.Clone(filler), tc.current...))
			is.True(changed)
		})
	}
}
