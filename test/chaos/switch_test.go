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

//go:build chaos

package chaos

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
)

// TestSIGKILL_TypesSwitch kills the source between the first record emitted
// under a new type configuration and its ack (design: "kill -9 at the mode
// switch"). The restart reads the old stamp again, so it must be refused
// without types.acceptChangeFrom and must continue without a gap with it; every
// position written after the switch carries the strict stamp.
func TestSIGKILL_TypesSwitch(t *testing.T) {
	db, err := sql.Open("mysql", testDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	h := &harness{t: t, dir: t.TempDir(), table: "chaos_switch", db: db}
	h.exec("DROP TABLE IF EXISTS " + h.table)
	h.exec("CREATE TABLE " + h.table + " (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(32) NOT NULL)")
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+h.table) })

	// A legacy pipeline (types.mode unset) that reached CDC.
	h.insertRange(1, 10)
	c := h.start("store_id:10")
	h.waitParked(c)
	h.kill(c)
	c = h.start("store_id:11")
	h.waitOpened(c)
	h.insertRange(11, 11)
	h.waitParked(c)
	h.kill(c)

	h.insertRange(12, 40) // one rows event

	// Switch, and die before the first record under the new stamp is acked.
	h.types = "types.mode=strict,types.acceptChangeFrom=legacy"
	c = h.start("append:1")
	h.waitParked(c)
	h.kill(c)

	// The token is still required: the stored position has the old stamp.
	h.types = "types.mode=strict"
	c = h.start("")
	select {
	case err := <-c.done:
		if err == nil || !strings.Contains(c.log.String(), "mysql.types.stamp_mismatch") {
			t.Fatalf("restart without the token: want a mysql.types.stamp_mismatch refusal, got %v\n%s", err, c.log.String())
		}
	case <-time.After(60 * time.Second):
		t.Fatal("restart without the token did not refuse within 60s")
	}

	h.types = "types.mode=strict,types.acceptChangeFrom=legacy"
	c = h.start("")
	h.waitLedgerHas(c, 40)
	if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := <-c.done; err != nil {
		t.Fatalf("final run did not stop cleanly: %v\n%s", err, c.log.String())
	}

	verify(t, h.dir, h.table, 40)

	positions, err := readLog(filepath.Join(h.dir, positionLogFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range positions {
		pos, err := common.ParseSDKPosition([]byte(p.Pos))
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case p.Run <= 2 && pos.Types != nil:
			t.Errorf("run %d: a legacy run wrote a stamped position", p.Run)
		case p.Run >= 3 && pos.Types.Name() != "strict/string/long":
			t.Errorf("run %d: position after the switch has stamp %s", p.Run, pos.Types.Name())
		}
	}
}
