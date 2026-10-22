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
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/conduitio/conduit-commons/dialer"
	"github.com/matryer/is"
)

var errDialRefused = errors.New("dial refused by test")

// refusingDialer records every call and refuses it.
type refusingDialer struct {
	mu    sync.Mutex
	calls []string
}

func (r *refusingDialer) dial(_ context.Context, network, address string) (net.Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, network+" "+address)
	return nil, errDialRefused
}

func (r *refusingDialer) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *refusingDialer) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// idleListener is a real listener that must never see a connection.
func idleListener(t *testing.T) (addr string, accepted *atomic.Int32) {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted = new(atomic.Int32)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			_ = c.Close()
		}
	}()
	t.Cleanup(func() { _ = l.Close() })
	return l.Addr().String(), accepted
}

func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func TestSource_Open_HostDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := idleListener(t)
	rec := &refusingDialer{}
	ctx := dialer.WithDialContext(context.Background(), rec.dial)

	s := &Source{config: SourceConfig{Config: Config{DSN: "u:p@tcp(" + addr + ")/d"}, Tables: []string{"*"}}}
	is.NoErr(s.config.Validate(ctx))

	err := s.Open(ctx, nil)
	is.True(errors.Is(err, errDialRefused))
	is.Equal(rec.all(), []string{"tcp " + addr})
	is.Equal(accepted.Load(), int32(0))

	// The pool the source holds keeps dialing through the host dialer.
	is.True(errors.Is(s.db.PingContext(context.Background()), errDialRefused))
	is.Equal(rec.count(), 2)
	is.NoErr(s.Teardown(context.Background()))
}

func TestSource_Open_NoDialerUnchanged(t *testing.T) {
	is := is.New(t)
	s := &Source{config: SourceConfig{Config: Config{DSN: "u:p@tcp(" + closedAddr(t) + ")/d"}, Tables: []string{"*"}}}
	is.NoErr(s.config.Validate(context.Background()))

	err := s.Open(context.Background(), nil)
	var opErr *net.OpError
	is.True(errors.As(err, &opErr)) // the driver's own dial, not a host dialer
	is.NoErr(s.Teardown(context.Background()))
}

func TestDestination_Open_HostDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := idleListener(t)
	rec := &refusingDialer{}
	ctx := dialer.WithDialContext(context.Background(), rec.dial)

	d := &Destination{config: DestinationConfig{Config: Config{DSN: "u:p@tcp(" + addr + ")/d"}}}
	is.NoErr(d.config.Validate(ctx))

	err := d.Open(ctx)
	is.True(errors.Is(err, errDialRefused))
	is.Equal(rec.all(), []string{"tcp " + addr})
	is.Equal(accepted.Load(), int32(0))
}

func TestDestination_Open_NoDialerUnchanged(t *testing.T) {
	is := is.New(t)
	d := &Destination{config: DestinationConfig{Config: Config{DSN: "u:p@tcp(" + closedAddr(t) + ")/d"}}}
	is.NoErr(d.config.Validate(context.Background()))

	var opErr *net.OpError
	is.True(errors.As(d.Open(context.Background()), &opErr))
}

// The snapshot's dedicated lock connection and its fetch workers draw from the
// source's pool, so they dial through the host dialer too.
func TestLockTables_HostDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := idleListener(t)
	rec := &refusingDialer{}
	ctx := dialer.WithDialContext(context.Background(), rec.dial)

	db, err := common.OpenDSN(ctx, "u:p@tcp("+addr+")/d")
	is.NoErr(err)
	defer db.Close()

	_, err = lockTables(context.Background(), db, []string{"t1", "t2"})
	is.True(errors.Is(err, errDialRefused))
	_, err = lockTables(context.Background(), db, []string{"t1"})
	is.True(errors.Is(err, errDialRefused))
	is.Equal(rec.all(), []string{"tcp " + addr, "tcp " + addr})
	is.Equal(accepted.Load(), int32(0))
}

// The cdc iterator's canal connects while it is built (binlog format check),
// through the host dialer. The binlog syncer and reconnects are covered by
// TestHostDialer_AllConnectionsIntegration, which needs a real server.
func TestCdcIterator_HostDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := idleListener(t)
	rec := &refusingDialer{}
	ctx := dialer.WithDialContext(context.Background(), rec.dial)

	s := &Source{config: SourceConfig{Config: Config{DSN: "u:p@tcp(" + addr + ")/d"}, Tables: []string{"*"}}}
	is.NoErr(s.config.Validate(ctx))

	_, err := newCdcIterator(ctx, cdcIteratorConfig{
		mysqlConfig:         s.config.MysqlCfg(),
		tables:              []string{"d.t"},
		disableCanalLogging: true,
	})
	is.True(err != nil && strings.Contains(err.Error(), errDialRefused.Error()))
	is.True(rec.count() >= 1)
	for _, c := range rec.all() {
		is.Equal(c, "tcp "+addr)
	}
	is.Equal(accepted.Load(), int32(0))
}
