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
	"sort"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/matryer/is"
)

func TestSource_getTableKeys(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := testutils.TestContext(t)

	// Define test tables
	type Table1 struct {
		ID   int    `gorm:"primaryKey"`
		Data string `gorm:"size:100"`
	}

	type Table2 struct {
		ID   int    `gorm:"primaryKey"`
		Data string `gorm:"size:100"`
	}

	type MetaStart struct {
		ID   int    `gorm:"primaryKey"`
		Data string `gorm:"size:100"`
	}
	type TestMetaMid struct {
		ID   int    `gorm:"primaryKey"`
		Data string `gorm:"size:100"`
	}
	type EndMeta struct {
		ID   int    `gorm:"primaryKey"`
		Data string `gorm:"size:100"`
	}

	// Get table names as they appear in MySQL
	table1Name := testutils.TableName(is, db, &Table1{})
	table2Name := testutils.TableName(is, db, &Table2{})
	startMetaName := testutils.TableName(is, db, &MetaStart{})
	midMetaName := testutils.TableName(is, db, &TestMetaMid{})
	endMetaName := testutils.TableName(is, db, &EndMeta{})

	testutils.CreateTables(is, db, &Table1{}, &Table2{}, &MetaStart{}, &TestMetaMid{}, &EndMeta{})

	type TestCase struct {
		name                 string
		tablePatterns        []string
		expectedTables       []string
		expectedTableRegexes []string
	}

	testCases := []*TestCase{
		{
			name:           "include all tables with wildcard",
			tablePatterns:  []string{"*"},
			expectedTables: []string{table1Name, table2Name, startMetaName, midMetaName, endMetaName},
		},
		{
			name:           "include specific table",
			tablePatterns:  []string{table1Name},
			expectedTables: []string{table1Name},
		},
		{
			name:           "exclude tables ending with meta",
			tablePatterns:  []string{"*", "-.*meta$"},
			expectedTables: []string{table1Name, table2Name, startMetaName, midMetaName},
		},
		{
			name:           "exclude all meta tables but include specific one",
			tablePatterns:  []string{"*", "-.*meta", "+" + midMetaName},
			expectedTables: []string{table1Name, table2Name, midMetaName},
		},
		{
			name:           "include table1 and table2",
			tablePatterns:  []string{table1Name, table2Name},
			expectedTables: []string{table1Name, table2Name},
		},
		{
			name:           "include all tables then exclude specific one",
			tablePatterns:  []string{"*", "-" + table1Name},
			expectedTables: []string{table2Name, startMetaName, midMetaName, endMetaName},
		},
		{
			name:           "No Match",
			tablePatterns:  []string{"doesnt_exist"},
			expectedTables: []string{},
		},
	}

	for _, testCase := range testCases {
		sort.Strings(testCase.expectedTables)
		testCase.expectedTableRegexes = createCanalRegexes("meroxadb", testCase.expectedTables)
	}

	createSource := func(tables, snapshotTables, cdcTables []string) *Source {
		source := &Source{
			config: SourceConfig{
				TableConfig:    map[string]TableConfig{},
				Tables:         tables,
				SnapshotTables: snapshotTables,
				CDCTables:      cdcTables,
			},
			db: db.SqlxDB,
		}
		return source
	}

	assertKeys := func(keys connectorTableKeys, testCase *TestCase) {
		snapshotTables := keys.Snapshot.GetTables()
		sort.Strings(snapshotTables)

		cdcTables := keys.Cdc.TableKeys.GetTables()
		sort.Strings(cdcTables)

		sort.Strings(keys.Cdc.TableRegexes)

		is.Equal(snapshotTables, testCase.expectedTables)
		is.Equal(cdcTables, testCase.expectedTables)
		is.Equal(keys.Cdc.TableRegexes, testCase.expectedTableRegexes)
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			is := is.New(t)

			source := createSource(testCase.tablePatterns, nil, nil)
			keys, err := source.getTableKeys(ctx, "meroxadb")
			is.NoErr(err)

			assertKeys(keys, testCase)
		})
		t.Run(testCase.name+"_snapshot", func(t *testing.T) {
			is := is.New(t)
			source := createSource(testCase.tablePatterns, testCase.tablePatterns, nil)

			keys, err := source.getTableKeys(ctx, "meroxadb")
			is.NoErr(err)

			assertKeys(keys, testCase)
		})
		t.Run(testCase.name+"_cdc", func(t *testing.T) {
			is := is.New(t)
			source := createSource(testCase.tablePatterns, nil, testCase.tablePatterns)

			keys, err := source.getTableKeys(ctx, "meroxadb")
			is.NoErr(err)

			assertKeys(keys, testCase)
		})
	}
}

func TestSource_RegexParseRule(t *testing.T) {
	is := is.New(t)

	testCases := []struct {
		name        string
		rule        string
		wantAction  Action
		wantPattern string
		wantErr     bool
	}{
		{
			name:        "include with plus prefix",
			rule:        "+users.*",
			wantAction:  Include,
			wantPattern: "users.*",
		},
		{
			name:        "exclude with minus prefix",
			rule:        "-.*meta$",
			wantAction:  Exclude,
			wantPattern: ".*meta$",
		},
		{
			name:        "include without prefix",
			rule:        "users",
			wantAction:  Include,
			wantPattern: "users",
		},
		{
			name:       "empty pattern with plus",
			rule:       "+",
			wantAction: Include,
			wantErr:    true,
		},
		{
			name:    "empty rule",
			rule:    "",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			action, pattern, err := ParseRule(tc.rule)
			if tc.wantErr {
				is.True(err != nil)
				return
			}

			is.NoErr(err)
			is.Equal(action, tc.wantAction)
			is.Equal(pattern, tc.wantPattern)
		})
	}
}
