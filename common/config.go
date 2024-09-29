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
	// URL is the connection string for the Mysql database.
	URL string `json:"url" validate:"required"`
}

//go:generate paramgen -output=paramgen_src.go SourceConfig

type SourceConfig struct {
	Config

	// Tables to be snapshotted. By default, all tables are included, but can be modified by adding a comma-separated string of regex patterns. They are applied in the order that they are provided, so the final regex supersedes all previous ones. To set an "include" regex, add "+" or nothing in front of the regex. To set an "exclude" regex, add "-" in front of the regex. e.g. "-.*meta$, wp_postmeta" will include all tables (by default), but exclude all tables ending with "meta" but include the table "wp_postmeta"
	Tables []string `json:"tables"`

	// DisableCanalLogs disables verbose logs.
	DisableCanalLogs bool `json:"disableCanalLogs"`

	// FetchSize limits how many rows should be retrieved on each database fetch.
	FetchSize int `json:"fetchSize" default:"50000"`
}

const (
	DefaultFetchSize = 50000
	// AllTablesWildcard can be used if you'd like to listen to all tables.
	AllTablesWildcard = "*"
)
