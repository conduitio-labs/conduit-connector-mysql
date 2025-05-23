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
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func TestSource_Teardown(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestSource_FilterTables(t *testing.T) {
	is := is.New(t)

	primaryKeys := common.PrimaryKeys{"id"}
	tableKeys := common.TableKeys{
		"table":    primaryKeys,
		"Atable":   primaryKeys,
		"tableA":   primaryKeys,
		"distinct": primaryKeys,
	}

	testCases := []struct {
		name     string
		cfg      SourceConfig
		filtered filteredTableKeys
	}{
		{
			name: "snapshot patterns do override",
			cfg: SourceConfig{
				Tables:         []string{"*", "-table"},
				SnapshotTables: []string{"tableA", "Atable"},
			},
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"tableA": primaryKeys,
					"Atable": primaryKeys,
				},
				Cdc: common.TableKeys{
					"distinct": primaryKeys,
				},
			},
		},
		{
			name: "cdc patterns do override",
			cfg: SourceConfig{
				Tables:    []string{"*", "-table"},
				CDCTables: []string{"tableA", "Atable"},
			},
			filtered: filteredTableKeys{
				Snapshot: common.TableKeys{
					"distinct": primaryKeys,
				},
				Cdc: common.TableKeys{
					"tableA": primaryKeys,
					"Atable": primaryKeys,
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			is := is.New(t)
			source := &Source{config: testCase.cfg}

			filtered, err := source.filterTables(tableKeys)
			is.NoErr(err)

			is.Equal("", cmp.Diff(filtered.Snapshot, testCase.filtered.Snapshot))
			is.Equal("", cmp.Diff(filtered.Cdc, testCase.filtered.Cdc))
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
				is.True(err != nil)
				if tc.expectedErrMsg != "" {
					is.Equal(tc.expectedErrMsg, err.Error()) // expected specific error message
				}
			} else {
				is.NoErr(err)
				areSlicesEq(is, actual, tc.expected)
			}
		})
	}
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
