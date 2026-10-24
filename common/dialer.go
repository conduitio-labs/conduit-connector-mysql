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

package common

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/conduitio/conduit-commons/dialer"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// OpenDSN opens a connection pool for dsn. When the host supplied a dialer on
// ctx (see the conduit-commons dialer package), every connection the pool ever
// opens, including reconnects after the pool is idle or a connection is
// discarded, is established through it. The dialer is captured here, so later
// contexts do not matter. Without a host dialer this is exactly sqlx.Open.
//
// The dialer is installed through mysql.Config.DialFunc on a driver connector
// owned by the returned pool, not through the driver's process-global
// RegisterDialContext table, so concurrent connectors with different dialers
// cannot collide and nothing needs deregistering on teardown. TLS is layered by
// the driver over the connection the dialer returns.
func OpenDSN(ctx context.Context, dsn string) (*sqlx.DB, error) {
	dial, ok := dialer.FromContext(ctx)
	if !ok {
		return openPlain(dsn)
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}
	return openWithDialer(cfg, dial)
}

// OpenConfig is like OpenDSN for an already parsed config. cfg is not modified.
func OpenConfig(ctx context.Context, cfg *mysql.Config) (*sqlx.DB, error) {
	dial, ok := dialer.FromContext(ctx)
	if !ok {
		return openPlain(cfg.FormatDSN())
	}
	return openWithDialer(cfg, dial)
}

func openWithDialer(cfg *mysql.Config, dial dialer.DialContextFunc) (*sqlx.DB, error) {
	cfg = cfg.Clone()
	// Host dialer: all pool connections dial through it.
	cfg.DialFunc = dial
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create mysql connector: %w", err)
	}
	return sqlx.NewDb(sql.OpenDB(connector), "mysql"), nil
}

func openPlain(dsn string) (*sqlx.DB, error) {
	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql pool: %w", err)
	}
	return db, nil
}
