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
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysqlorg "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type CanalConfig struct {
	*mysql.Config
	Tables         []string
	DisableLogging bool
	Version        string
}

// Canal is a wrapper around the original *canal.Canal from go-mysql/canal
// v1.12.0. This fixes `canal.GetMasterPos()`, which did not work with mariadb:
//
//	https://github.com/go-mysql-org/go-mysql/issues/1029
//
// It got fixed here:
//
//	https://github.com/go-mysql-org/go-mysql/pull/1030
//
// Until go-mysql-org/go-mysql v1.12.1 or later is released, we can just backport the fix here.
//
// NOTE: We should be able to easily remove this wrapper, the utility functions
// added, and their tests once we upgrade to the new version.
type Canal struct {
	*canal.Canal
	flavor, serverVersion string
}

func NewCanal(ctx context.Context, config CanalConfig) (*Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Addr
	cfg.User = config.User
	cfg.Password = config.Passwd
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

	version, flavor, err := parseVersion(config.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to parse version: %w", err)
	}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql canal: %w", err)
	}

	c.GetMasterPos()

	return &Canal{
		Canal:         c,
		flavor:        flavor,
		serverVersion: version,
	}, nil
}

func (c *Canal) GetMasterPos() (mysqlPos gomysqlorg.Position, err error) {
	query := getShowBinaryLogQuery(c.flavor, c.serverVersion)

	rr, err := c.Execute(query)
	if err != nil {
		return mysqlPos, fmt.Errorf("failed to execute %q: %w", query, err)
	}

	name, _ := rr.GetString(0, 0)
	pos, _ := rr.GetInt(0, 1)

	return gomysqlorg.Position{Name: name, Pos: uint32(pos)}, nil
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

func getShowBinaryLogQuery(flavor, serverVersion string) string {
	switch flavor {
	case gomysqlorg.MariaDBFlavor:
		eq, err := gomysqlorg.CompareServerVersions(serverVersion, "10.5.2")
		if (err == nil) && (eq >= 0) {
			return "SHOW BINLOG STATUS"
		}
	case gomysqlorg.MySQLFlavor:
		eq, err := gomysqlorg.CompareServerVersions(serverVersion, "8.4.0")
		if (err == nil) && (eq >= 0) {
			return "SHOW BINARY LOG STATUS"
		}
	}

	return "SHOW MASTER STATUS"
}

func parseVersion(rawVersion string) (version, flavor string, err error) {
	parts := strings.Split(rawVersion, "-")
	if len(parts) == 0 {
		return "", "", fmt.Errorf("invalid empty version string")
	}

	if strings.Contains(rawVersion, "MariaDB") {
		flavor = gomysqlorg.MariaDBFlavor
		if strings.HasPrefix(rawVersion, "5.5.5-") && len(parts) > 1 {
			version = parts[1]
		} else {
			version = parts[0]
		}
	} else {
		flavor = gomysqlorg.MySQLFlavor
		version = parts[0]
	}

	if version == "" {
		return "", "", fmt.Errorf("could not extract version from: %q", rawVersion)
	}

	_, err = gomysqlorg.CompareServerVersions(version, "0.0.0")
	if err != nil {
		return "", "", fmt.Errorf("extracted version %q from %q is not valid: %w", version, rawVersion, err)
	}

	return version, flavor, nil
}
