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
	"net"
	"sync/atomic"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/dialer"
	"github.com/matryer/is"
)

// TestHostDialer_AllConnectionsIntegration runs a snapshot plus CDC source
// against a real server with a dialer that delegates to the network, and checks
// that every connection the server accepted for the source was made through the
// dialer: the server's own connection counter must grow by exactly the number of
// dial calls. That covers the pool, the snapshot lock connection, canal's
// metadata connection and the binlog syncer, and a reconnect after the pool's
// idle connections are dropped.
func TestHostDialer_AllConnectionsIntegration(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)

	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	user1 := testutils.InsertUser(is, db, 1)

	serverConnections := func() int {
		var name string
		var n int
		is.NoErr(db.SqlxDB.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE 'Connections'").Scan(&name, &n))
		return n
	}
	serverConnections() // warm the helper pool so the probe itself dials nothing new
	before := serverConnections()

	var dials atomic.Int32
	var nd net.Dialer
	dial := func(c context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return nd.DialContext(c, network, address)
	}
	dctx := dialer.WithDialContext(ctx, dial)

	source := testSourceOpen(dctx, is, map[string]string{"tables": "users"}, nil)

	testutils.ReadAndAssertSnapshot(ctx, is, source, user1)
	user2 := testutils.InsertUser(is, db, 2)
	testutils.ReadAndAssertCreate(ctx, is, source, user2)

	// Drop the pool's idle connections: the next query reconnects, and must
	// dial again even though the context it runs under has no dialer.
	atRest := dials.Load()
	source.db.SetMaxIdleConns(0)
	is.NoErr(source.db.PingContext(context.Background()))
	is.True(dials.Load() > atRest)

	is.NoErr(source.Teardown(ctx))

	after := serverConnections()
	is.True(dials.Load() >= 3) // pool, canal metadata, binlog syncer
	is.Equal(after-before, int(dials.Load()))
}
