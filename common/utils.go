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
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/siddontang/go-log/log"
)

func FormatValue(val any) any {
	switch val := val.(type) {
	case time.Time:
		return val.UTC()
	case *time.Time:
		return val.UTC()
	case []uint8:
		s := string(val)
		if parsed, err := time.Parse(time.DateTime, s); err == nil {
			return parsed.UTC().Format(time.RFC3339)
		}
		return s
	case uint64:
		if val <= math.MaxInt64 {
			return int64(val)
		}

		// this will make avro encoding fail, as it doesn't support uint64.
		return val
	default:
		return val
	}
}

type CanalConfig struct {
	*mysql.Config
	Tables         []string
	DisableLogging bool
}

func NewCanal(ctx context.Context, config CanalConfig) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Addr
	cfg.User = config.User
	cfg.Password = config.Passwd

	cfg.IncludeTableRegex = config.Tables
	if config.DisableLogging {
		cfg.Logger = log.NewDefault(&log.NullHandler{})
	} else {
		cfg.Logger = zerologCanalLogger{sdk.Logger(ctx)}
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

type zerologCanalLogger struct {
	logger *zerolog.Logger
}

func (z zerologCanalLogger) Debug(args ...any) {
	z.logger.Debug().Msg(fmt.Sprint(args...))
}

func (z zerologCanalLogger) Debugf(format string, args ...any) {
	z.logger.Debug().Msgf(format, args...)
}

func (z zerologCanalLogger) Debugln(args ...any) {
	z.logger.Debug().Msg(fmt.Sprintln(args...))
}

func (z zerologCanalLogger) Error(args ...any) {
	z.logger.Error().Msg(fmt.Sprint(args...))
}

func (z zerologCanalLogger) Errorf(format string, args ...any) {
	z.logger.Error().Msgf(format, args...)
}

func (z zerologCanalLogger) Errorln(args ...any) {
	z.logger.Error().Msg(fmt.Sprintln(args...))
}

func (z zerologCanalLogger) Fatal(args ...any) {
	z.logger.Fatal().Msg(fmt.Sprint(args...))
}

func (z zerologCanalLogger) Fatalf(format string, args ...any) {
	z.logger.Fatal().Msgf(format, args...)
}

func (z zerologCanalLogger) Fatalln(args ...any) {
	z.logger.Fatal().Msg(fmt.Sprintln(args...))
}

func (z zerologCanalLogger) Info(args ...any) {
	z.logger.Info().Msg(fmt.Sprint(args...))
}

func (z zerologCanalLogger) Infof(format string, args ...any) {
	z.logger.Info().Msgf(format, args...)
}

func (z zerologCanalLogger) Infoln(args ...any) {
	z.logger.Info().Msg(fmt.Sprintln(args...))
}

func (z zerologCanalLogger) Panic(args ...any) {
	z.logger.Panic().Msg(fmt.Sprint(args...))
}

func (z zerologCanalLogger) Panicf(format string, args ...any) {
	z.logger.Panic().Msgf(format, args...)
}

func (z zerologCanalLogger) Panicln(args ...any) {
	z.logger.Panic().Msg(fmt.Sprintln(args...))
}

func (z zerologCanalLogger) Print(args ...any) {
	z.logger.Info().Msg(fmt.Sprint(args...))
}

func (z zerologCanalLogger) Printf(format string, args ...any) {
	z.logger.Info().Msgf(format, args...)
}

func (z zerologCanalLogger) Println(args ...any) {
	z.logger.Info().Msg(fmt.Sprintln(args...))
}

func (z zerologCanalLogger) Warn(args ...any) {
	z.logger.Warn().Msg(fmt.Sprint(args...))
}

func (z zerologCanalLogger) Warnf(format string, args ...any) {
	z.logger.Warn().Msgf(format, args...)
}

func (z zerologCanalLogger) Warnln(args ...any) {
	z.logger.Warn().Msg(fmt.Sprintln(args...))
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

func AreEqual(a, b any) (equal bool, cantCompare bool) {
	aStr, aIsStr := a.(string)
	bStr, bIsStr := b.(string)
	aBytes, aOk := a.([]uint8)
	bBytes, bOk := b.([]uint8)

	switch {
	case aIsStr && bOk:
		return aStr == string(bBytes), false
	case bIsStr && aOk:
		return string(aBytes) == bStr, false
	}

	defer func() {
		if err := recover(); err == nil {
			return
		}

		if aOk && bOk {
			equal = bytes.Equal(aBytes, bBytes)
			return
		}
		cantCompare = true
	}()

	return a == b, false
}
