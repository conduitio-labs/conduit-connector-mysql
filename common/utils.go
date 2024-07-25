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
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	mysqlorg "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
)

func FormatValue(val any) any {
	switch val := val.(type) {
	case time.Time:
		return val.UTC().Format(time.RFC3339)
	case *time.Time:
		return val.UTC().Format(time.RFC3339)
	case []uint8:
		return string(val)
	default:
		return val
	}
}

// Canal is a wrapper around *canal.Canal to enforce our custom CdcPosition.
type Canal struct{ *canal.Canal }

func (c *Canal) RunFrom(pos CdcPosition) error {
	mysqlPos := mysqlorg.Position{
		Name: pos.Name,
		Pos:  pos.Pos,
	}

	//nolint:wrapcheck // Canal wraps *canal.Canal, no need for more error wrapping
	return c.Canal.RunFrom(mysqlPos)
}

func (c *Canal) GetMasterPos() (CdcPosition, error) {
	pos, err := c.Canal.GetMasterPos()
	cdcPosition := CdcPosition{
		Name: pos.Name,
		Pos:  pos.Pos,
	}

	//nolint:wrapcheck // Canal wraps *canal.Canal, no need for more error wrapping
	return cdcPosition, err
}

func NewCanal(config *mysql.Config, tables []string) (*Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Addr
	cfg.User = config.User
	cfg.Password = config.Passwd

	cfg.IncludeTableRegex = tables

	// Disable dumping
	cfg.Dump.ExecutionPath = ""
	cfg.ParseTime = true

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql canal: %w", err)
	}

	return &Canal{c}, nil
}
