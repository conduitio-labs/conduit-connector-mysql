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
	sdk "github.com/conduitio/conduit-connector-sdk"
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
