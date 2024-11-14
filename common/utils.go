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
	"reflect"
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
		return string(val)
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

// IsGreaterOrEqual uses reflection to apply the >= operation to two values of any type.
// This function is useful for determining when to stop the snapshot iteration.
//
// Manually checking each type can be error-prone. In production environments,
// the default fetch size is quite large, so the latency of IsGreaterOrEqual is
// negligible compared to the latency of fetching rows from the database.
func IsGreaterOrEqual(a, b any) (greaterOrEqual bool, cantCompare bool) {
	defer func() {
		if err := recover(); err != nil {
			cantCompare = true
		}
	}()

	va := reflect.ValueOf(a)
	vb := reflect.ValueOf(b)

	switch {
	case va.Kind() == reflect.String || va.Type() == reflect.TypeOf([]uint8{}):
		aStr := toString(a)
		bStr := toString(b)
		if aStr != nil && bStr != nil {
			return *aStr >= *bStr, false
		}
		return false, true
	case vb.Kind() == reflect.String || vb.Type() == reflect.TypeOf([]uint8{}):
		return false, true
	case va.Type() == reflect.TypeOf(time.Time{}) && vb.Type() == reflect.TypeOf(time.Time{}):
		//nolint:forcetypeassert // checked already
		aTime := a.(time.Time)
		//nolint:forcetypeassert // checked already
		bTime := b.(time.Time)
		return aTime.After(bTime) || aTime.Equal(bTime), false
	}

	if !isNumber(va.Kind()) || !isNumber(vb.Kind()) {
		return false, true
	}

	af, ok1 := toFloat64(va)
	bf, ok2 := toFloat64(vb)
	if !ok1 || !ok2 {
		return false, true
	}

	return af >= bf, false
}

func isNumber(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func toFloat64(v reflect.Value) (float64, bool) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		return v.Float(), true
	default:
		return 0, false
	}
}

func toString(v any) *string {
	switch v := v.(type) {
	case string:
		return &v
	case []uint8:
		s := string(v)
		return &s
	default:
		return nil
	}
}
