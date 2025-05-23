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
	"sort"
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

	sort.Strings(s1)
	sort.Strings(s2)
	if s1 == nil {
		s1 = []string{}
	}
	if s2 == nil {
		s2 = []string{}
	}

	is.Equal(s1, s2)
}

func TestSource_FilterTables(t *testing.T) {
	is := is.New(t)
	ctx := testutils.TestContext(t)

	primaryKeys := common.PrimaryKeys{"id"}

	tableKeys := common.TableKeys{
		"table":    primaryKeys,
		"Atable":   primaryKeys,
		"tableA":   primaryKeys,
		"distinct": primaryKeys,
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
				Tables: []string{"*"},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"table":    primaryKeys,
					"Atable":   primaryKeys,
					"tableA":   primaryKeys,
					"distinct": primaryKeys,
				},
				Cdc: common.TableKeys{
					"table":    primaryKeys,
					"Atable":   primaryKeys,
					"tableA":   primaryKeys,
					"distinct": primaryKeys,
				},
			},
		},
		{
			name: "include specific table",
			cfg: SourceConfig{
				Tables: []string{"Atable"},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"Atable": primaryKeys,
				},
				Cdc: common.TableKeys{
					"Atable": primaryKeys,
				},
			},
		},
		{
			name: "snapshot overrides tables pattern",
			cfg: SourceConfig{
				Tables:         []string{"*"},
				SnapshotTables: []string{"table", "Atable"},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"table":  primaryKeys,
					"Atable": primaryKeys,
				},
				Cdc: common.TableKeys{
					"table":    primaryKeys,
					"Atable":   primaryKeys,
					"tableA":   primaryKeys,
					"distinct": primaryKeys,
				},
			},
		},
		{
			name: "cdc overrides tables pattern",
			cfg: SourceConfig{
				Tables:    []string{"*"},
				CDCTables: []string{"distinct"},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"table":    primaryKeys,
					"Atable":   primaryKeys,
					"tableA":   primaryKeys,
					"distinct": primaryKeys,
				},
				Cdc: common.TableKeys{
					"distinct": primaryKeys,
				},
			},
		},
		{
			name: "both snapshot and cdc override tables pattern",
			cfg: SourceConfig{
				Tables:         []string{"*"},
				SnapshotTables: []string{"table"},
				CDCTables:      []string{"Atable", "distinct"},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"table": primaryKeys,
				},
				Cdc: common.TableKeys{
					"Atable":   primaryKeys,
					"distinct": primaryKeys,
				},
			},
		},
		{
			name: "exclude tables ending with A using snapshot override",
			cfg: SourceConfig{
				Tables:         []string{"*"},
				SnapshotTables: []string{"*", "-.*A$"},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"Atable":   primaryKeys,
					"table":    primaryKeys,
					"distinct": primaryKeys,
				},
				Cdc: common.TableKeys{
					"table":    primaryKeys,
					"Atable":   primaryKeys,
					"tableA":   primaryKeys,
					"distinct": primaryKeys,
				},
			},
		},
		{
			name: "empty tables config",
			cfg: SourceConfig{
				Tables: []string{},
			},
			tableKeys: tableKeys,
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{},
				Cdc:      common.TableKeys{},
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
			areSlicesEq(is, filtered.Cdc.GetTables(), testCase.filtered.Cdc.GetTables())
		})
	}
}

func TestFilterTables(t *testing.T) {
	is := is.New(t)

	tableNames := []string{"table", "Atable", "tableA", "distinct"}

	testCases := []struct {
		name           string
		rules          []string
		tableNames     []string
		expected       []string
		expectErr      bool
		expectedErrMsg string
	}{
		{
			name:       "include all tables with wildcard",
			rules:      []string{"*"},
			tableNames: tableNames,
			expected:   []string{"Atable", "table", "tableA", "distinct"},
		},
		{
			name:       "include specific table",
			rules:      []string{"Atable"},
			tableNames: tableNames,
			expected:   []string{"Atable"},
		},
		{
			name:       "exclude tables ending with A",
			rules:      []string{"*", "-.*A$"},
			tableNames: tableNames,
			expected:   []string{"Atable", "table", "distinct"},
		},
		{
			name:       "exclude all tables but include specific one",
			rules:      []string{"*", "-.*", "+tableA"},
			tableNames: tableNames,
			expected:   []string{"tableA"},
		},
		{
			name:       "include table and Atable",
			rules:      []string{"^table$", "Atable"},
			tableNames: tableNames,
			expected:   []string{"table", "Atable"},
		},
		{
			name:       "include all tables then exclude specific one",
			rules:      []string{"*", "-distinct"},
			tableNames: tableNames,
			expected:   []string{"Atable", "table", "tableA"},
		},
		{
			name:       "No Match",
			rules:      []string{"doesnt_exist"},
			tableNames: tableNames,
			expected:   []string{},
		},
		{
			name:       "empty rules",
			rules:      []string{},
			tableNames: tableNames,
			expected:   []string{},
		},
		{
			name:           "invalid regex pattern",
			rules:          []string{"[invalid"},
			tableNames:     tableNames,
			expectErr:      true,
			expectedErrMsg: "invalid regex pattern: error parsing regexp: missing closing ]: `[invalid`",
		},
		{
			name:           "invalid rule format",
			rules:          []string{"-"}, // rule too short
			tableNames:     tableNames,
			expectErr:      true,
			expectedErrMsg: "invalid rule format: -",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			actual, err := filterTables(tc.rules, tc.tableNames)

			if tc.expectErr {
				is.True(err != nil) // expected an error
				if tc.expectedErrMsg != "" {
					is.Equal(tc.expectedErrMsg, err.Error()) // expected specific error message
				}
			} else {
				is.NoErr(err) // expected no error
				areSlicesEq(is, actual, tc.expected)
			}
		})
	}
}
