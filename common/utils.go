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
	"github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog"
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

type CanalConfig struct {
	*mysql.Config
	Tables         []string
	DisableLogging bool
	Logger         *zerolog.Logger
}

func NewCanal(config CanalConfig) (*canal.Canal, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = config.Addr
	cfg.User = config.User
	cfg.Password = config.Passwd

	cfg.IncludeTableRegex = config.Tables
	if config.DisableLogging {
		cfg.Logger = nopLogger{}
	} else {
		cfg.Logger = zerologCanalLogger{config.Logger}
	}

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
