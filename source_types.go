// Copyright © 2026 Meroxa, Inc.
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
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

// legacyRemovalNotice is logged on every start in legacy mode. The release and
// date are fixed when the first release with types.mode is cut (design:
// "Legacy lifecycle").
const legacyRemovalNotice = CodeLegacyTypes + ": legacy type mode is deprecated and will be removed in the " +
	"second minor release after the one that introduced types.mode; set types.mode=strict (for a pipeline in CDC, " +
	"with types.acceptChangeFrom=legacy)"

// openTypes resolves the type configuration at Open and runs the checks that
// go with it. See resolveTypes for the transition table.
func (s *Source) openTypes(ctx context.Context, positioned bool, pos common.Position) (*typeSettings, error) {
	phase := phaseNone
	switch {
	case pos.CdcPosition != nil:
		phase = phaseCDC
	case pos.SnapshotPosition != nil:
		phase = phaseSnapshot
	}

	res, err := resolveTypes(s.config.explicitTypes(), positioned, pos.Types, phase)
	if err != nil {
		return nil, err
	}
	cfg := res.config
	logger := sdk.Logger(ctx)

	catalog := newColumnCatalog(s.db, s.config.MysqlCfg().DBName)
	if err := catalog.load(ctx, s.config.Tables); err != nil {
		if cfg.strict() {
			return nil, err
		}
		logger.Warn().Err(err).Msg("could not read column information for the legacy type warnings")
	}
	columns := catalog.snapshot()
	if cfg.strict() {
		for _, table := range s.config.Tables {
			if len(columns[table]) == 0 {
				return nil, newError(ErrNullabilityUnavailable, CodeNullabilityUnavailable, "tables",
					"grant SELECT on the table to the connector's user, or exclude it",
					"table %s has no rows in information_schema.columns, so the nullability of its columns is unknown", table)
			}
		}
	}

	if err := checkBinlogSettings(ctx, s.db, cfg.strict()); err != nil {
		return nil, err
	}

	logger.Info().
		Str("types.mode", cfg.mode).
		Str("types.decimal", cfg.decimal).
		Str("types.bigintUnsigned", cfg.bigintUnsigned).
		Str("position_types", pos.Types.Name()).
		Str("phase", phase.String()).
		Msg("resolved type configuration")
	for _, w := range res.warnings {
		logger.Warn().Msg(w)
	}
	if res.switchedFrom != "" {
		logger.Warn().
			Str("from", res.switchedFrom).
			Str("to", cfg.name()).
			Msgf("types stamp changed from %s to %s; keep types.acceptChangeFrom until the log says a record under "+
				"the new configuration was acknowledged", res.switchedFrom, cfg.name())
	}
	if !cfg.strict() {
		for _, w := range legacyColumnWarnings(s.config.Tables, columns) {
			logger.Warn().Msg(w)
		}
		logger.Warn().Msg(legacyRemovalNotice)
	} else if cfg.decimal == typesDecimalDouble {
		for _, table := range s.config.Tables {
			for _, col := range sortedColumns(columns[table]) {
				if columns[table][col].isDecimal() {
					logger.Warn().Msgf("types.decimal=double: column %s.%s is delivered as a double and loses precision", table, col)
				}
			}
		}
	}

	return newTypeSettings(cfg, phase, res.switchedFrom, catalog), nil
}

// binlogSetting is one server variable the connector's row events depend on.
type binlogSetting struct {
	name, code, want string
	// ok reports whether the value is acceptable; an absent variable is "".
	ok func(string) bool
}

var binlogSettings = []binlogSetting{
	{name: "binlog_format", code: CodeBinlogFormat, want: "ROW", ok: func(v string) bool {
		return v == "" || strings.EqualFold(v, "ROW")
	}},
	{name: "binlog_row_image", code: CodeBinlogRowImage, want: "FULL", ok: func(v string) bool {
		return v == "" || strings.EqualFold(v, "FULL")
	}},
	{name: "binlog_row_value_options", code: CodeBinlogRowValueOptions, want: "''", ok: func(v string) bool {
		return v == ""
	}},
}

// checkBinlogSettings reads the global binlog variables. Under a value other
// than the one listed, row events do not carry whole rows (or there are none),
// so strict mode refuses to start; legacy mode warns (design decision 4).
// A variable the server does not have (MySQL 5.7, MariaDB) counts as unset.
func checkBinlogSettings(ctx context.Context, db *sqlx.DB, strict bool) error {
	for _, setting := range binlogSettings {
		var name, value string
		// SHOW cannot take a placeholder; the names are constants above.
		err := db.QueryRowxContext(ctx, "SHOW GLOBAL VARIABLES LIKE '"+setting.name+"'").Scan(&name, &value)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			value = ""
		case err != nil:
			return fmt.Errorf("read %s: %w", setting.name, err)
		}
		if setting.ok(value) {
			continue
		}
		e := newError(ErrBinlogSettings, setting.code, setting.name,
			fmt.Sprintf("run SET PERSIST %s = %s on the server (or set it in the parameter group of a managed "+
				"service) and reconnect the writers", setting.name, setting.want),
			"the server's %s is %q; row events then do not carry the full row", setting.name, value)
		if strict {
			return e
		}
		sdk.Logger(ctx).Warn().Msg(e.Error() + " (legacy mode: a warning in this version, refused in a later one)")
	}
	return nil
}
