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
	"fmt"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/jmoiron/sqlx"
)

func newSqlxDB(config common.Config) (*sqlx.DB, error) {
	dataSourceName := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		config.User, config.Password, config.Host, config.Port, config.Database,
	)
	db, err := sqlx.Open("mysql", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}
	return db, nil
}

func newCanal(config SourceConfig) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", config.Host, config.Port)
	cfg.User = config.User
	cfg.Password = config.Password

	cfg.IncludeTableRegex = config.Tables

	// Disable dumping
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql canal: %w", err)
	}

	return c, nil
}
