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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/dialer"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

var errDialRefused = errors.New("dial refused by test")

// recorder is a dialer that records every call and refuses it.
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) dial(_ context.Context, network, address string) (net.Conn, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, network+" "+address)
	return nil, errDialRefused
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// idleListener is a real listener that must never see a connection when the
// dialer refuses.
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

func TestOpenDSN_HostDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := idleListener(t)
	rec := &recorder{}
	ctx := dialer.WithDialContext(context.Background(), rec.dial)

	db, err := OpenDSN(ctx, "u:p@tcp("+addr+")/d")
	is.NoErr(err)
	defer db.Close()

	// The first connection goes through the dialer, with the configured address.
	err = db.PingContext(context.Background())
	is.True(errors.Is(err, errDialRefused))
	is.Equal(rec.snapshot(), []string{"tcp " + addr})

	// A failed connection is not pooled: the next one dials again, and so does
	// a second connection held at the same time.
	_, err = db.Conn(context.Background())
	is.True(errors.Is(err, errDialRefused))
	_, err = db.Conn(context.Background())
	is.True(errors.Is(err, errDialRefused))
	is.Equal(len(rec.snapshot()), 3)

	is.Equal(accepted.Load(), int32(0)) // nothing reached the real listener
}

// The dialer is captured when the pool is built; a later context does not
// matter for reconnects.
func TestOpenDSN_HostDialerCapturedAtOpen(t *testing.T) {
	is := is.New(t)
	rec := &recorder{}
	ctx, cancel := context.WithCancel(dialer.WithDialContext(context.Background(), rec.dial))
	db, err := OpenDSN(ctx, "u:p@tcp(db.example.test:3306)/d")
	is.NoErr(err)
	defer db.Close()
	cancel()

	err = db.PingContext(context.Background())
	is.True(errors.Is(err, errDialRefused))
	is.Equal(rec.snapshot(), []string{"tcp db.example.test:3306"})
}

func TestOpenConfig_HostDialerDoesNotMutateConfig(t *testing.T) {
	is := is.New(t)
	cfg := mustParse(t, "u:p@tcp(db.example.test:3306)/d")
	rec := &recorder{}
	db, err := OpenConfig(dialer.WithDialContext(context.Background(), rec.dial), cfg)
	is.NoErr(err)
	defer db.Close()
	is.True(cfg.DialFunc == nil)
	is.True(errors.Is(db.PingContext(context.Background()), errDialRefused))
}

func TestOpenDSN_NoDialerUnchanged(t *testing.T) {
	is := is.New(t)
	// A closed port: the connection is attempted by the driver's own dialer and
	// fails with a plain network error.
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	is.NoErr(err)
	addr := l.Addr().String()
	is.NoErr(l.Close())

	db, err := OpenDSN(context.Background(), "u:p@tcp("+addr+")/d")
	is.NoErr(err)
	defer db.Close()
	err = db.PingContext(context.Background())
	is.True(err != nil)
	is.True(!errors.Is(err, errDialRefused))
	var opErr *net.OpError
	is.True(errors.As(err, &opErr))

	cfg := mustParse(t, "u:p@tcp("+addr+")/d")
	db2, err := OpenConfig(context.Background(), cfg)
	is.NoErr(err)
	defer db2.Close()
	is.True(errors.As(db2.PingContext(context.Background()), &opErr))
}

func TestNewCanal_HostDialer(t *testing.T) {
	is := is.New(t)
	addr, accepted := idleListener(t)
	rec := &recorder{}
	ctx := dialer.WithDialContext(context.Background(), rec.dial)

	c, err := NewCanal(ctx, CanalConfig{
		Config:         mustParse(t, "u:p@tcp("+addr+")/d"),
		Tables:         []string{"d.t"},
		DisableLogging: true,
	})
	if err != nil {
		// canal may connect while it is built; the dialer must have been used.
		is.True(errors.Is(err, errDialRefused) || strings.Contains(err.Error(), errDialRefused.Error()))
		is.True(len(rec.snapshot()) >= 1)
		is.Equal(accepted.Load(), int32(0))
		return
	}
	defer c.Close()

	// Metadata/SHOW connection (Execute, GetMasterPos): dials, and dials again
	// on the next call since no connection was established.
	_, err = c.GetMasterPos()
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), errDialRefused.Error()))
	first := len(rec.snapshot())
	is.True(first >= 1)
	_, err = c.GetMasterPos()
	is.True(err != nil)
	is.True(len(rec.snapshot()) > first) // reconnect dials again

	// Binlog syncer connection: dials through the same function.
	before := len(rec.snapshot())
	err = c.RunFrom(posZero())
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), errDialRefused.Error()))
	is.True(len(rec.snapshot()) > before)

	for _, call := range rec.snapshot() {
		is.Equal(call, "tcp "+addr)
	}
	is.Equal(accepted.Load(), int32(0))
}

func TestNewCanal_NoDialerUnchanged(t *testing.T) {
	is := is.New(t)
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	is.NoErr(err)
	addr := l.Addr().String()
	is.NoErr(l.Close())

	c, err := NewCanal(context.Background(), CanalConfig{
		Config:         mustParse(t, "u:p@tcp("+addr+")/d"),
		Tables:         []string{"d.t"},
		DisableLogging: true,
	})
	if err != nil {
		is.True(!strings.Contains(err.Error(), errDialRefused.Error()))
		return
	}
	defer c.Close()
	_, err = c.GetMasterPos()
	is.True(err != nil)
	is.True(strings.Contains(err.Error(), "refused") || strings.Contains(err.Error(), "connect"))
}

// The binlog/replication connection must inherit the DSN's TLS: canal dials it
// separately from the sqlx pool, and without this it would stream the binlog in
// plaintext while the snapshot and destination used TLS.
func TestBuildCanalConfig_TLSFromDSN(t *testing.T) {
	is := is.New(t)

	tlsCfg := buildCanalConfig(CanalConfig{Config: mustParse(t, "u:p@tcp(db.example.test:3306)/d?tls=skip-verify")})
	is.True(tlsCfg.TLSConfig != nil)
	is.True(tlsCfg.TLSConfig.InsecureSkipVerify)

	plain := buildCanalConfig(CanalConfig{Config: mustParse(t, "u:p@tcp(db.example.test:3306)/d")})
	is.True(plain.TLSConfig == nil) // without tls= the binlog connection stays plaintext, as before
}

// TestOpenDSN_TLSLayeredOverDialedConn proves the driver runs its TLS handshake
// over the connection the host dialer returned (not over a connection it dialed
// itself): a minimal server on the dialer's side of a pipe advertises SSL, reads
// the SSL request, and completes a TLS handshake there.
func TestOpenDSN_TLSLayeredOverDialedConn(t *testing.T) {
	is := is.New(t)
	cert := selfSigned(t)

	var dialed atomic.Int32
	handshakeDone := make(chan error, 1)
	dial := func(ctx context.Context, _, _ string) (net.Conn, error) {
		dialed.Add(1)
		client, server := net.Pipe()
		go func() {
			handshakeDone <- fakeServer(ctx, server, cert)
		}()
		return client, nil
	}
	ctx := dialer.WithDialContext(context.Background(), dial)
	db, err := OpenDSN(ctx, "u:p@tcp(db.example.test:3306)/d?tls=skip-verify")
	is.NoErr(err)
	defer db.Close()

	ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = db.PingContext(ctx2) // fails after TLS: the fake server stops there
	select {
	case err := <-handshakeDone:
		is.NoErr(err) // TLS handshake completed on the dialed connection
	case <-time.After(5 * time.Second):
		t.Fatal("no TLS handshake on the dialed connection")
	}
	is.Equal(dialed.Load(), int32(1))
}

func fakeServer(ctx context.Context, c net.Conn, cert tls.Certificate) error {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	// Initial handshake packet, protocol v10, advertising SSL.
	var p []byte
	p = append(p, 10)
	p = append(p, "8.0.0\x00"...)
	p = append(p, 1, 0, 0, 0)                     // connection id
	p = append(p, "abcdefgh"...)                  // auth data 1
	p = append(p, 0)                              // filler
	p = append(p, 0x00, 0x8a)                     // caps lower: PROTOCOL_41|SSL|SECURE_CONN
	p = append(p, 45)                             // charset
	p = append(p, 2, 0)                           // status
	p = append(p, 0x08, 0x00)                     // caps upper: PLUGIN_AUTH
	p = append(p, 21)                             // auth data len
	p = append(p, make([]byte, 10)...)            // reserved
	p = append(p, "ijklmnopqrst\x00"...)          // auth data 2
	p = append(p, "mysql_native_password\x00"...) // plugin
	hdr := []byte{byte(len(p)), byte(len(p) >> 8), byte(len(p) >> 16), 0}
	if _, err := c.Write(append(hdr, p...)); err != nil {
		return fmt.Errorf("write handshake: %w", err)
	}
	// SSL request: 32-byte payload.
	h := make([]byte, 4)
	if _, err := io.ReadFull(c, h); err != nil {
		return fmt.Errorf("read ssl request header: %w", err)
	}
	n := int(binary.LittleEndian.Uint32(append(h[:3:3], 0)))
	if _, err := io.ReadFull(c, make([]byte, n)); err != nil {
		return fmt.Errorf("read ssl request: %w", err)
	}
	if err := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}}).HandshakeContext(ctx); err != nil {
		return fmt.Errorf("tls handshake: %w", err)
	}
	return nil
}

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func mustParse(t *testing.T, dsn string) *mysql.Config {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func posZero() gomysql.Position { return gomysql.Position{Name: "mysql-bin.000001", Pos: 4} }
