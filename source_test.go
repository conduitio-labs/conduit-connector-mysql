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
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/matryer/is"
)

func TestSource_Teardown(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func areSlicesEq(is *is.I, s1, s2 []string) {
	is.Helper()

	m1, m2 := map[string]int{}, map[string]int{}
	for _, s := range s1 {
		m1[s] = 0
	}
	for _, s := range s2 {
		m2[s] = 0
	}

	is.Equal(s1, s2)
}

func TestSource_FilterTables(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	primaryKeys := common.PrimaryKeys{"id"}

	tableKeys := common.TableKeys{
		"table":       primaryKeys,
		"Atable":      primaryKeys,
		"tableA":      primaryKeys,
		"unmatchable": primaryKeys,
	}

	testCases := []struct {
		name      string
		cfg       SourceConfig
		tableKeys common.TableKeys
		filtered  filteredTableKeys
	}{
		{
			name: "include all tables with wildcard",
			cfg: SourceConfig{
				Tables:         []string{"*"},
				SnapshotTables: []string{},
				CDCTables:      []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"Atable":      primaryKeys,
					"table":       primaryKeys,
					"tableA":      primaryKeys,
					"unmatchable": primaryKeys,
				},
				Cdc: CdcTableKeys{
					TableKeys: common.TableKeys{
						"Atable":      primaryKeys,
						"table":       primaryKeys,
						"tableA":      primaryKeys,
						"unmatchable": primaryKeys,
					},
					TableRegexes: createCanalRegexes(testutils.Database, []string{"Atable", "table", "tableA", "unmatchable"}),
				},
			},
		},
		{
			name: "include specific table",
			cfg: SourceConfig{
				Tables:         []string{"table"},
				SnapshotTables: []string{},
				CDCTables:      []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{"table": primaryKeys},
				Cdc: CdcTableKeys{
					TableKeys:    common.TableKeys{"table": primaryKeys},
					TableRegexes: createCanalRegexes(testutils.Database, []string{"table"}),
				},
			},
		},
		{
			name: "exclude tables ending with A",
			cfg: SourceConfig{
				Tables:         []string{"*", "-.*A$"},
				SnapshotTables: []string{},
				CDCTables:      []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{"Atable": primaryKeys, "table": primaryKeys, "unmatchable": primaryKeys},
				Cdc: CdcTableKeys{
					TableKeys:    common.TableKeys{"Atable": primaryKeys, "table": primaryKeys, "unmatchable": primaryKeys},
					TableRegexes: createCanalRegexes(testutils.Database, []string{"Atable", "table", "unmatchable"}),
				},
			},
		},
		{
			name: "exclude all tables but include specific one",
			cfg: SourceConfig{
				Tables:         []string{"*", "-.*", "+tableA"},
				SnapshotTables: []string{},
				CDCTables:      []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{"tableA": primaryKeys},
				Cdc: CdcTableKeys{
					TableKeys:    common.TableKeys{"tableA": primaryKeys},
					TableRegexes: createCanalRegexes(testutils.Database, []string{"tableA"}),
				},
			},
		},
		{
			name: "include table and Atable",
			cfg: SourceConfig{
				Tables:         []string{"table", "Atable"},
				SnapshotTables: []string{},
				CDCTables:      []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{"table": primaryKeys, "Atable": primaryKeys},
				Cdc: CdcTableKeys{
					TableKeys:    common.TableKeys{"table": primaryKeys, "Atable": primaryKeys},
					TableRegexes: createCanalRegexes(testutils.Database, []string{"table", "Atable"}),
				},
			},
		},
		{
			name: "include all tables then exclude specific one",
			cfg: SourceConfig{
				Tables:         []string{"*", "-table"},
				SnapshotTables: []string{},
				CDCTables:      []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{"Atable": primaryKeys, "tableA": primaryKeys, "unmatchable": primaryKeys},
				Cdc: CdcTableKeys{
					TableKeys:    common.TableKeys{"Atable": primaryKeys, "tableA": primaryKeys, "unmatchable": primaryKeys},
					TableRegexes: createCanalRegexes(testutils.Database, []string{"Atable", "tableA", "unmatchable"}),
				},
			},
		},
		{
			name: "No Match",
			cfg: SourceConfig{
				Tables:         []string{"doesnt_exist"},
				SnapshotTables: []string{},
				CDCTables:      []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{},
				Cdc:      CdcTableKeys{TableKeys: common.TableKeys{}, TableRegexes: []string{}},
			},
		},
		{
			name: "snapshot tables override with regex",
			cfg: SourceConfig{
				Tables:         []string{"*"},       // Should be ignored for snapshot
				SnapshotTables: []string{"table.*"}, // Regex to include tables starting with "table"
				CDCTables:      []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{"table": primaryKeys, "tableA": primaryKeys},
				Cdc: CdcTableKeys{
					TableKeys:    common.TableKeys{"Atable": primaryKeys, "table": primaryKeys, "tableA": primaryKeys, "unmatchable": primaryKeys}, // Falls back to Tables
					TableRegexes: createCanalRegexes(testutils.Database, []string{"Atable", "table", "tableA", "unmatchable"}),
				},
			},
		},
		{
			name: "cdc tables override with regex",
			cfg: SourceConfig{
				Tables:         []string{"*", "-unmatchable"}, // Should be ignored for cdc
				SnapshotTables: []string{},
				CDCTables:      []string{".*table$"}, // Regex to include tables ending with "table"
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{"Atable": primaryKeys, "table": primaryKeys, "tableA": primaryKeys, "unmatchable": primaryKeys}, // Falls back to Tables
				Cdc: CdcTableKeys{
					TableKeys:    common.TableKeys{"Atable": primaryKeys, "table": primaryKeys},
					TableRegexes: createCanalRegexes(testutils.Database, []string{"Atable", "table"}),
				},
			},
		},
		{
			name: "snapshot and cdc tables override with different regexes",
			cfg: SourceConfig{
				Tables:         []string{"*"},           // Should be ignored for both
				SnapshotTables: []string{".*A$"},        // Regex to include tables ending with "A"
				CDCTables:      []string{"unmatchable"}, // Specific table for cdc
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{"tableA": primaryKeys, "Atable": primaryKeys},
				Cdc: CdcTableKeys{
					TableKeys:    common.TableKeys{"unmatchable": primaryKeys},
					TableRegexes: createCanalRegexes(testutils.Database, []string{"unmatchable"}),
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			is := is.New(t)
			source := &Source{config: testCase.cfg}

			filtered, err := source.filterTables(ctx, testutils.Database, testCase.tableKeys)
			is.NoErr(err)

			areSlicesEq(is, filtered.Snapshot.GetTables(), testCase.filtered.Snapshot.GetTables())
			areSlicesEq(is, filtered.Cdc.TableKeys.GetTables(), testCase.filtered.Cdc.TableKeys.GetTables())
			areSlicesEq(is, filtered.Cdc.TableRegexes, testCase.filtered.Cdc.TableRegexes)
		})
	}
}
