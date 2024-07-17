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
	"github.com/jmoiron/sqlx"
)

func FormatValue(val any) any {
	switch val := val.(type) {
	case time.Time:
		return val.UTC().Format(time.RFC3339)
	case *time.Time:
		return val.UTC().Format(time.RFC3339)
	case []uint8:
		s := string(val)
		if parsed, err := time.Parse(time.DateTime, s); err == nil {
			return parsed.UTC().Format(time.RFC3339)
		}
		return s
	default:
		return val
	}
}

func NewSqlxDB(config Config) (*sqlx.DB, error) {
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

func NewCanal(config SourceConfig) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", config.Host, config.Port)
	cfg.User = config.User
	cfg.Password = config.Password

	cfg.IncludeTableRegex = config.Tables
	cfg.Logger = nopLogger{}

	// Disable dumping
	cfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql canal: %w", err)
	}

	return c, nil
}

// nopLogger disables logging for go-mysql/canal.
type nopLogger struct{}

func (n nopLogger) Debug(...any)          {}
func (n nopLogger) Debugf(string, ...any) {}
func (n nopLogger) Debugln(...any)        {}
func (n nopLogger) Error(...any)          {}
func (n nopLogger) Errorf(string, ...any) {}
func (n nopLogger) Errorln(...any)        {}
func (n nopLogger) Fatal(...any)          {}
func (n nopLogger) Fatalf(string, ...any) {}
func (n nopLogger) Fatalln(...any)        {}
func (n nopLogger) Info(...any)           {}
func (n nopLogger) Infof(string, ...any)  {}
func (n nopLogger) Infoln(...any)         {}
func (n nopLogger) Panic(...any)          {}
func (n nopLogger) Panicf(string, ...any) {}
func (n nopLogger) Panicln(...any)        {}
func (n nopLogger) Print(...any)          {}
func (n nopLogger) Printf(string, ...any) {}
func (n nopLogger) Println(...any)        {}
func (n nopLogger) Warn(...any)           {}
func (n nopLogger) Warnf(string, ...any)  {}
func (n nopLogger) Warnln(...any)         {}
