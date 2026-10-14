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
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
)

// MaxListedTables is the largest table set a TableCoverage stores by name.
// Above it the set is stored as a hash and a count, because the coverage rides
// on every CDC record (about 30 bytes per name).
const MaxListedTables = 200

// TableCoverage records, in every CDC position, which tables' existing rows the
// pipeline is known to have covered, so a restart can tell a table added later
// from one it already handled. Without it, skipping the snapshot on a CDC
// restart silently drops the pre-existing rows of any table added to the
// configuration after CDC began. Invariant 3.
//
// A nil *TableCoverage on CdcPosition means a position written before this
// field existed (coverage unknown). A non-nil, empty one means known: nothing
// has been snapshotted. The field is additive: builds that do not know it
// ignore it and drop it on their next write.
//
// Three disjoint sets in list form:
//   - Snapshotted: the pipeline snapshotted these tables.
//   - Unverified: a position from an older build was adopted for these tables;
//     whether they were snapshotted could not be verified. The connector keeps
//     warning about them on every start.
//   - CDCOnly: these tables were added by a pattern after CDC began. Their
//     existing rows were NOT snapshotted. Warned about once, then recorded.
//
// Above MaxListedTables the sets are replaced by Hash (over the sorted union)
// and Count, with HasUnverified keeping the warning alive.
type TableCoverage struct {
	Snapshotted []string `json:"snapshotted,omitempty"`
	Unverified  []string `json:"unverified,omitempty"`
	CDCOnly     []string `json:"cdc_only,omitempty"`

	Hash          string `json:"hash,omitempty"`
	Count         int    `json:"count,omitempty"`
	HasUnverified bool   `json:"has_unverified,omitempty"`
}

// NewTableCoverage builds a coverage from the three sets, choosing list or hash
// form by the size of their union. The inputs need not be sorted.
func NewTableCoverage(snapshotted, unverified, cdcOnly []string) *TableCoverage {
	snapshotted, unverified, cdcOnly = sortedSet(snapshotted), sortedSet(unverified), sortedSet(cdcOnly)
	union := sortedSet(slices.Concat(snapshotted, unverified, cdcOnly))

	if len(union) > MaxListedTables {
		return &TableCoverage{Hash: hashTables(union), Count: len(union), HasUnverified: len(unverified) > 0}
	}
	return &TableCoverage{Snapshotted: snapshotted, Unverified: unverified, CDCOnly: cdcOnly}
}

// Hashed reports whether the coverage is in hash form (names not stored).
func (c *TableCoverage) Hashed() bool { return c.Hash != "" }

// Known returns the sorted union of covered tables. nil in hash form.
func (c *TableCoverage) Known() []string {
	if c.Hashed() {
		return nil
	}
	return sortedSet(slices.Concat(c.Snapshotted, c.Unverified, c.CDCOnly))
}

// HasUnverifiedTables reports whether any unverified table remains.
func (c *TableCoverage) HasUnverifiedTables() bool {
	return c.HasUnverified || len(c.Unverified) > 0
}

// Added returns the tables in current that the coverage does not know. In hash
// form the names are not available: changed reports that current differs from
// the recorded set (a table was added, or removed; the two cannot be told
// apart), and added is nil.
func (c *TableCoverage) Added(current []string) (added []string, changed bool) {
	current = sortedSet(current)
	if c.Hashed() {
		return nil, hashTables(current) != c.Hash
	}
	known := c.Known()
	for _, t := range current {
		if _, found := slices.BinarySearch(known, t); !found {
			added = append(added, t)
		}
	}
	return added, len(added) > 0
}

// Restrict returns the coverage limited to the tables in current: a table that
// left the configuration is forgotten, so adding it back later is detected as an
// addition rather than assumed covered. In hash form the set cannot be
// restricted and the coverage is returned as is.
func (c *TableCoverage) Restrict(current []string) *TableCoverage {
	if c.Hashed() {
		return c
	}
	keep := func(in []string) []string {
		var out []string
		for _, t := range in {
			if slices.Contains(current, t) {
				out = append(out, t)
			}
		}
		return out
	}
	return NewTableCoverage(keep(c.Snapshotted), keep(c.Unverified), keep(c.CDCOnly))
}

// WithCDCOnly returns the coverage with the given tables recorded as CDC-only.
// In hash form the new hash is that of current, the full table set, which is
// the set the restart is now continuing with.
func (c *TableCoverage) WithCDCOnly(added, current []string) *TableCoverage {
	if c.Hashed() {
		return &TableCoverage{Hash: hashTables(sortedSet(current)), Count: len(sortedSet(current)), HasUnverified: c.HasUnverified}
	}
	return NewTableCoverage(c.Snapshotted, c.Unverified, slices.Concat(c.CDCOnly, added))
}

func sortedSet(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := slices.Clone(in)
	slices.Sort(out)
	return slices.Compact(out)
}

func hashTables(sorted []string) string {
	sum := sha256.Sum256([]byte(strings.Join(sorted, "\n")))
	return hex.EncodeToString(sum[:])
}
