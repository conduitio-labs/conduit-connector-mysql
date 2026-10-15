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
	"errors"
	"strings"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

func withDSNParam(dsn, param string) string {
	if strings.Contains(dsn, "?") {
		return dsn + "&" + param
	}
	return dsn + "?" + param
}

// Under sjis, cp932, gbk, gb18030 and big5 a character can end in the byte
// 0x60, so a doubled backtick can be read as part of the previous character
// and end the quoted name. Such DSNs are refused when the config is validated.
func TestDestination_Validate_RefusesUnsafeCharset(t *testing.T) {
	for _, param := range []string{
		"charset=sjis",
		"charset=utf8mb4,gbk",
		"collation=gbk_chinese_ci",
		"character_set_client=big5",
		"character_set_client=%27cp932%27",
	} {
		t.Run(param, func(t *testing.T) {
			ctx := testutils.TestContext(t)
			is := is.New(t)
			dest := &Destination{}
			err := sdk.Util.ParseConfig(ctx, map[string]string{"dsn": withDSNParam(testutils.DSN, param)},
				dest.Config(), Connector.NewSpecification().DestinationParams)
			is.True(errors.Is(err, ErrUnsafeCharset))
		})
	}

	t.Run("utf8mb4 is accepted", func(t *testing.T) {
		ctx := testutils.TestContext(t)
		is := is.New(t)
		dest := &Destination{}
		is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{"dsn": withDSNParam(testutils.DSN, "charset=utf8mb4")},
			dest.Config(), Connector.NewSpecification().DestinationParams))
	})
}

// Open asks the connection for its character set, which also catches settings
// the DSN check cannot see (here: a session variable it does not parse).
func TestDestination_Open_RefusesUnsafeConnectionCharset(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	dest := &Destination{}
	dest.config.DSN = withDSNParam(testutils.DSN, "character_set_client=sjis") // Validate is bypassed on purpose
	err := dest.Open(ctx)
	is.True(errors.Is(err, ErrUnsafeCharset))
}

// Open checks one pooled connection. A connection opened later can come up
// with another character set (here: init_connect changed after Open, which
// applies to users without SUPER), so every Write checks its own transaction.
func TestDestination_Write_RefusesUnsafeCharsetOnNewConnection(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	cfg, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)
	is.NoErr(db.Exec("CREATE USER IF NOT EXISTS 'dest_plain'@'%' IDENTIFIED BY 'plainpass'").Error)
	is.NoErr(db.Exec("GRANT ALL ON " + cfg.DBName + ".* TO 'dest_plain'@'%'").Error)
	t.Cleanup(func() { db.Exec("DROP USER IF EXISTS 'dest_plain'@'%'") })

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_cs").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_cs (id BIGINT PRIMARY KEY, v VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_cs") })

	plain := *cfg
	plain.User, plain.Passwd = "dest_plain", "plainpass"
	dest := &Destination{}
	is.NoErr(sdk.Util.ParseConfig(ctx, map[string]string{"dsn": plain.FormatDSN()},
		dest.Config(), Connector.NewSpecification().DestinationParams))
	is.NoErr(dest.Open(ctx))
	t.Cleanup(func() { _ = dest.Teardown(ctx) })

	is.NoErr(db.Exec("SET GLOBAL init_connect='SET NAMES sjis'").Error)
	t.Cleanup(func() { db.Exec("SET GLOBAL init_connect=''") })
	dest.db.SetMaxIdleConns(0) // drop the checked connection; the next one runs init_connect

	written, err := dest.Write(ctx, []opencdc.Record{
		columnsTestRecord("dest_cs", opencdc.StructuredData{"id": 1, "v": "a"}),
	})
	is.True(errors.Is(err, ErrUnsafeCharset))
	is.Equal(written, 0)
	var count int64
	is.NoErr(db.Raw("SELECT COUNT(*) FROM dest_cs").Scan(&count).Error)
	is.Equal(count, int64(0))
}
