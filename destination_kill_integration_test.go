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
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

const killChildEnv = "MYSQL_DEST_KILL_CHILD"

// killBatch is three column runs; the second targets id 50, which the parent
// holds locked, so the child blocks after applying the first run.
func killBatch() []opencdc.Record {
	return []opencdc.Record{
		columnsTestRecord("dest_kill", opencdc.StructuredData{"id": 1, "v": "a"}),
		columnsTestRecord("dest_kill", opencdc.StructuredData{"id": 2, "v": "b"}),
		columnsTestRecord("dest_kill", opencdc.StructuredData{"id": 50, "v": "c", "extra": "x"}),
		columnsTestRecord("dest_kill", opencdc.StructuredData{"id": 3, "v": "d"}),
	}
}

// TestDestinationKillChild is the process the SIGKILL test kills. It only runs
// when started by that test.
func TestDestinationKillChild(t *testing.T) {
	if os.Getenv(killChildEnv) != "1" {
		t.Skip("run by TestDestination_SIGKILLMidTransaction")
	}
	sessionWaitTimeout = 3 * time.Second
	ctx := testutils.TestContext(t)
	is := is.New(t)
	dest, _ := testDestination(ctx, is)
	_, _ = dest.Write(ctx, killBatch())
	t.Fatal("Write returned; the test kills this process while Write is blocked")
}

// halfOpenProxy forwards TCP connections to target. When the client side
// closes, it keeps the server side open and stops reading from it, as a NAT, a
// proxy or a lost FIN does: the server never learns the client is gone.
type halfOpenProxy struct {
	ln    net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func newHalfOpenProxy(ctx context.Context, t *testing.T, target string) *halfOpenProxy {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &halfOpenProxy{ln: ln}
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, server)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(server, client) }() // client EOF: server side stays open
			go func() { _, _ = io.Copy(client, server) }()
		}
	}()
	t.Cleanup(p.close)
	return p
}

func (p *halfOpenProxy) close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

// The connector process is killed with SIGKILL in the middle of a write
// transaction that holds row locks, and its TCP connection is left half-open,
// so the server never sees the close. Nothing of the transaction may be
// committed (invariant 1: it was never acked), the server must end the session
// and release its locks within the session wait_timeout the destination sets
// (before this, such a session held them for the server default of 8 hours and
// the redelivery blocked), and the redelivered batch must apply completely and
// idempotently (invariant 3).
func TestDestination_SIGKILLMidTransaction(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)

	is.NoErr(db.Exec("DROP TABLE IF EXISTS dest_kill").Error)
	is.NoErr(db.Exec("CREATE TABLE dest_kill (id BIGINT PRIMARY KEY, v VARCHAR(10), extra VARCHAR(10))").Error)
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS dest_kill") })
	is.NoErr(db.Exec("INSERT INTO dest_kill VALUES (50, 'old', 'old')").Error)

	// Registered after the table cleanup so that it runs first: closing the
	// proxy ends a session the test left half-open, which would otherwise hold
	// a metadata lock and block the DROP.
	cfg, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)
	proxy := newHalfOpenProxy(ctx, t, cfg.Addr)
	childCfg := *cfg
	childCfg.Addr = proxy.ln.Addr().String()

	locker := db.Begin()
	is.NoErr(locker.Exec("SELECT * FROM dest_kill WHERE id = 50 FOR UPDATE").Error)
	lockerOpen := true
	t.Cleanup(func() {
		if lockerOpen {
			locker.Rollback()
		}
	})

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run", "^TestDestinationKillChild$", "-test.count=1", "-test.timeout=2m")
	cmd.Env = append(os.Environ(), killChildEnv+"=1", "MYSQL_TEST_DSN="+childCfg.FormatDSN())
	is.NoErr(cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }) // never outlives the test

	// Wait until the child is blocked on the lock: the first run is applied
	// inside its transaction, the second waits.
	waitFor(t, 60*time.Second, "the child to block on the row lock", func() bool {
		var waiting int64
		is.NoErr(db.Raw("SELECT COUNT(*) FROM performance_schema.data_lock_waits").Scan(&waiting).Error)
		return waiting > 0
	})

	is.NoErr(cmd.Process.Signal(syscall.SIGKILL))
	_ = cmd.Wait()
	is.NoErr(locker.Rollback().Error)
	lockerOpen = false

	// The half-open session must end within its wait_timeout (3s here), and
	// roll back.
	waitFor(t, 30*time.Second, "the server to end the killed session's transaction", func() bool {
		var open int64
		is.NoErr(db.Raw("SELECT COUNT(*) FROM information_schema.innodb_trx").Scan(&open).Error)
		return open == 0
	})
	rows := readColumnsRows(is, db, "dest_kill")
	is.Equal(len(rows), 1) // only the row that existed before
	is.Equal(rows[50], columnsRow{ID: 50, V: strp("old"), Extra: strp("old")})

	// Redelivery after the restart: applied completely, then again idempotently.
	dest, cleanup := testDestination(ctx, is)
	defer cleanup()
	for i := 0; i < 2; i++ {
		written, err := dest.Write(ctx, killBatch())
		is.NoErr(err)
		is.Equal(written, 4)
	}
	rows = readColumnsRows(is, db, "dest_kill")
	is.Equal(len(rows), 4)
	is.Equal(rows[50], columnsRow{ID: 50, V: strp("c"), Extra: strp("x")})
	is.Equal(rows[3], columnsRow{ID: 3, V: strp("d")})
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
