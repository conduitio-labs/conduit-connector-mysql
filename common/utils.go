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
	"context"
	"fmt"
	"io"
	"log/slog"
	"strconv"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type CanalConfig struct {
	*mysql.Config
	Tables         []string
	DisableLogging bool
	Flavor         string // "mysql" or "mariadb"
}

func NewCanal(ctx context.Context, config CanalConfig) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Addr
	cfg.User = config.User
	cfg.Password = config.Passwd
	cfg.Flavor = config.Flavor

	cfg.IncludeTableRegex = config.Tables
	if config.DisableLogging {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	} else {
		zerologLogger := sdk.Logger(ctx)
		cfg.Logger = slog.New(&zerologHandler{logger: zerologLogger})
	}

	// Disable dumping
	cfg.Dump.ExecutionPath = ""
	cfg.ParseTime = true

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql canal: %w", err)
	}

	return c, nil
}

type zerologHandler struct {
	logger *zerolog.Logger
}

func (h *zerologHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (h *zerologHandler) Handle(_ context.Context, r slog.Record) error {
	event := h.logger.With()

	r.Attrs(func(a slog.Attr) bool {
		event = event.Interface(a.Key, a.Value.Any())
		return true
	})

	logger := event.Logger()
	switch r.Level {
	case slog.LevelDebug:
		logger.Debug().Msg(r.Message)
	case slog.LevelInfo:
		logger.Info().Msg(r.Message)
	case slog.LevelWarn:
		logger.Warn().Msg(r.Message)
	case slog.LevelError:
		logger.Error().Msg(r.Message)
	}

	return nil
}

func (h *zerologHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	event := h.logger.With()
	for _, a := range attrs {
		event = event.Interface(a.Key, a.Value.Any())
	}
	logger := event.Logger()
	return &zerologHandler{logger: &logger}
}

func (h *zerologHandler) WithGroup(name string) slog.Handler {
	logger := h.logger.With().Str("group", name).Logger()
	return &zerologHandler{logger: &logger}
}

const ServerIDKey = "mysql.serverID"

func GetServerID(ctx context.Context, db *sqlx.DB) (string, error) {
	var serverIDRow struct {
		ServerID uint64 `db:"server_id"`
	}

	row := db.QueryRowxContext(ctx, "SELECT @@server_id as server_id")
	if err := row.StructScan(&serverIDRow); err != nil {
		return "", fmt.Errorf("failed to scan server id: %w", err)
	}

	serverID := strconv.FormatUint(serverIDRow.ServerID, 10)

	return serverID, nil
}

// PrimaryKeys contains all possible primary keys that a table can have. The
// order is important, so that we can properly build ORDER BY clauses.
type PrimaryKeys []string

// TableKeyFetcher fetches primary keys from the database and caches them.
type TableKeyFetcher struct {
	database string
	cache    map[string]PrimaryKeys
}

func NewTableKeyFetcher(database string) *TableKeyFetcher {
	return &TableKeyFetcher{
		database: database,
		cache:    make(map[string]PrimaryKeys),
	}
}

func (t *TableKeyFetcher) GetKeys(tx *sqlx.Tx, table string) (PrimaryKeys, error) {
	keys, ok := t.cache[table]
	if ok {
		return keys, nil
	}

	keys, err := GetPrimaryKeys(tx, t.database, table)
	if err != nil {
		return nil, err
	}

	t.cache[table] = keys

	return keys, nil
}

// keyQuerier allows us to select primary keys from both a sqlx.DB and an sqlx.Tx transaction.
type keyQuerier interface {
	Select(dest any, query string, args ...any) error
}

func GetPrimaryKeys(db keyQuerier, database, table string) (PrimaryKeys, error) {
	var primaryKeys []struct {
		ColumnName string `db:"COLUMN_NAME"`
	}

	err := db.Select(&primaryKeys, `
		SELECT COLUMN_NAME
		FROM information_schema.key_column_usage
		WHERE constraint_name = 'PRIMARY'
			AND table_schema = ?
			AND table_name = ?
		ORDER BY ORDINAL_POSITION
	`, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key(s) from table %s: %w", table, err)
	}

	keys := make(PrimaryKeys, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		keys = append(keys, pk.ColumnName)
	}

	return keys, nil
}
