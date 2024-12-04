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

type Config struct {
	// DSN is the connection string for the MySQL database.
	DSN string `json:"dsn" validate:"required"`
}

//go:generate paramgen -output=paramgen_src.go SourceConfig

type SourceConfig struct {
	Config

	// TableConfig holds the custom configuration that each table can have.
	TableConfig map[string]TableConfig `json:"tableConfig"`

	// Tables represents the tables to read from.
	Tables []string `json:"tables" validate:"required"`

	// DisableCanalLogs disables verbose logs.
	DisableCanalLogs bool `json:"disableCanalLogs"`

	// FetchSize limits how many rows should be retrieved on each database fetch.
	FetchSize uint64 `json:"fetchSize" default:"10000"`

	// UnsafeSnapshot allows a snapshot of a table with neither a primary key
	// nor a defined sorting column. The opencdc.Position won't record the last record
	// read from a table.
	UnsafeSnapshot bool `json:"unsafeSnapshot"`
}

type TableConfig struct {
	// SortingColumn allows to force using a custom column to sort the snapshot.
	SortingColumn string `json:"sortingColumn"`
}

const DefaultFetchSize = 50000

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

type DestinationConfig struct {
	Config

	// Table is used as the target table into which records are inserted.
	Table string `json:"table" validate:"required"`

	// Key is the primary key of the specified table.
	Key string `json:"key" validate:"required"`
}
