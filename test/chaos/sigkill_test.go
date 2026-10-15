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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	_ "github.com/go-sql-driver/mysql"
)

func TestMain(m *testing.M) {
	if os.Getenv(envChild) == "1" {
		os.Exit(runChild())
	}
	os.Exit(m.Run())
}

func testDSN() string {
	if v := os.Getenv(envDSN); v != "" {
		return v
	}
	return "root:meroxaadmin@tcp(127.0.0.1:3306)/meroxadb"
}

type harness struct {
	t     *testing.T
	dir   string
	table string
	db    *sql.DB
	run   int
}

type childProc struct {
	cmd  *exec.Cmd
	done chan error
	log  *bytes.Buffer
}

// start launches the next child, parking at park ("" for none).
func (h *harness) start(park string) *childProc {
	h.t.Helper()
	h.run++
	_ = os.Remove(filepath.Join(h.dir, parkedFile))
	_ = os.Remove(filepath.Join(h.dir, openedFile))

	cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^$")
	// A fixed environment: never the parent's, which can hold credentials.
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"TMPDIR=" + os.TempDir(),
		envChild + "=1",
		envDir + "=" + h.dir,
		envRun + "=" + strconv.Itoa(h.run),
		envTable + "=" + h.table,
		envPark + "=" + park,
		envDSN + "=" + testDSN(),
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("start child: %v", err)
	}
	c := &childProc{cmd: cmd, done: make(chan error, 1), log: &out}
	go func() { c.done <- cmd.Wait() }()
	h.t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			<-c.done
		}
	})
	h.t.Logf("run %d: started child pid %d, park %q", h.run, cmd.Process.Pid, park)
	return c
}

// waitParked waits until the child reports that it parked, or fails if it
// exits first.
func (h *harness) waitParked(c *childProc) {
	h.t.Helper()
	h.waitMarker(c, parkedFile, "park")
}

// waitOpened waits until the child's source is open.
func (h *harness) waitOpened(c *childProc) {
	h.t.Helper()
	h.waitMarker(c, openedFile, "open its source")
}

func (h *harness) waitMarker(c *childProc, file, what string) {
	h.t.Helper()
	deadline := time.After(90 * time.Second)
	for {
		select {
		case err := <-c.done:
			h.t.Fatalf("run %d: child exited before it did %s: %v\n%s", h.run, what, err, c.log.String())
		case <-deadline:
			h.t.Fatalf("run %d: child did not %s within 90s\n%s", h.run, what, c.log.String())
		case <-time.After(20 * time.Millisecond):
		}
		b, err := os.ReadFile(filepath.Join(h.dir, file))
		if err == nil && string(b) == strconv.Itoa(h.run) {
			return
		}
	}
}

// kill sends SIGKILL and reaps the child.
func (h *harness) kill(c *childProc) {
	h.t.Helper()
	if err := c.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		h.t.Fatalf("SIGKILL: %v", err)
	}
	<-c.done
	h.t.Logf("run %d: killed (SIGKILL)", h.run)
}

func (h *harness) exec(q string, args ...any) {
	h.t.Helper()
	if _, err := h.db.ExecContext(context.Background(), q, args...); err != nil {
		h.t.Fatalf("%s: %v", q, err)
	}
}

// insertRange inserts ids [from, to] in one statement, which the binlog logs as
// one rows event.
func (h *harness) insertRange(from, to int64) {
	h.t.Helper()
	var b strings.Builder
	b.WriteString("INSERT INTO " + h.table + " (id, v) VALUES ")
	for id := from; id <= to; id++ {
		if id > from {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "(%d, 'v-%d')", id, id)
	}
	h.exec(b.String())
}

func (h *harness) ledger() []entry {
	h.t.Helper()
	l, err := readLog(filepath.Join(h.dir, ledgerFile))
	if err != nil {
		h.t.Fatal(err)
	}
	return l
}

// waitLedgerHas waits until the ledger holds every id in [1, n].
func (h *harness) waitLedgerHas(c *childProc, n int64) {
	h.t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-c.done:
			h.t.Fatalf("run %d: child exited early: %v\n%s", h.run, err, c.log.String())
		default:
		}
		seen := map[int64]bool{}
		for _, e := range h.ledger() {
			seen[e.ID] = true
		}
		missing := int64(0)
		for id := int64(1); id <= n; id++ {
			if !seen[id] {
				missing++
			}
		}
		if missing == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.t.Fatalf("run %d: ledger still misses rows after 120s", h.run)
}

// TestSIGKILL_ResumeWithoutLossOrRedelivery kills the source, as a process,
// mid-snapshot, mid-checkpoint, at the snapshot/CDC boundary, inside a
// multi-row binlog event and at a random instant, and checks invariants 2 and
// 3 and exact resume over the whole history. See doc.go.
func TestSIGKILL_ResumeWithoutLossOrRedelivery(t *testing.T) {
	db, err := sql.Open("mysql", testDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	dir := t.TempDir()
	if keep := os.Getenv("MYSQL_CHAOS_KEEP_DIR"); keep != "" {
		// For debugging: keep the ledger and position log.
		dir = keep
		_ = os.RemoveAll(dir)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	h := &harness{t: t, dir: dir, table: "chaos_sigkill", db: db}
	h.exec("DROP TABLE IF EXISTS " + h.table)
	h.exec("CREATE TABLE " + h.table + " (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(32) NOT NULL)")
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+h.table) })

	const snapshotRows = 300 // fetchSize is 50: six chunks
	h.insertRange(1, snapshotRows)

	// Snapshot: inside a chunk, before the batch is acked.
	c := h.start("append:120")
	h.waitParked(c)
	h.kill(c)

	// Snapshot, mid-checkpoint: acked, position not stored.
	c = h.start("ack:60")
	h.waitParked(c)
	h.kill(c)

	// The boundary: the last snapshot row's position is stored, no change yet.
	c = h.start("store_id:" + strconv.Itoa(snapshotRows))
	h.waitParked(c)
	h.kill(c)

	// Into change capture: a row written after Open is read by CDC, and
	// storing its position moves the pipeline to the CDC phase. (Rows
	// written while no source runs would be read by the resumed snapshot,
	// whose end is taken at Open.)
	const marker = snapshotRows + 1
	c = h.start("store_id:" + strconv.Itoa(marker))
	h.waitOpened(c)
	h.insertRange(marker, marker)
	h.waitParked(c)
	h.kill(c)

	// CDC: one 500-row statement (several rows events after one Table_map),
	// then single-row events.
	h.insertRange(marker+1, marker+500)
	for id := int64(marker + 501); id <= marker+550; id++ {
		h.insertRange(id, id)
	}
	const total = marker + 550

	// Inside the 500-row event, before the batch is acked.
	c = h.start("append:250")
	h.waitParked(c)
	h.kill(c)

	// Inside the event again, mid-checkpoint.
	c = h.start("ack:100")
	h.waitParked(c)
	h.kill(c)

	// A kill at an instant nobody chose, once the child is delivering.
	before := len(h.ledger())
	c = h.start("")
	for len(h.ledger()) == before {
		select {
		case err := <-c.done:
			t.Fatalf("child exited: %v\n%s", err, c.log.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	time.Sleep(time.Duration(rand.Intn(40)) * time.Millisecond)
	h.kill(c)

	// More changes while nothing runs, then a final run that must deliver
	// everything and stop gracefully on SIGTERM.
	for id := int64(total + 1); id <= total+20; id++ {
		h.insertRange(id, id)
	}
	c = h.start("")
	h.waitLedgerHas(c, total+20)
	if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-c.done:
		if err != nil {
			t.Fatalf("final run did not stop cleanly on SIGTERM: %v\n%s", err, c.log.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("final run did not stop within 30s of SIGTERM (invariant 7)\n%s", c.log.String())
	}

	verify(t, h.dir, h.table, total+20)
}

// verify checks the whole history in dir. See doc.go for the properties.
func verify(t *testing.T, dir, table string, rows int64) {
	t.Helper()
	ledger, err := readLog(filepath.Join(dir, ledgerFile))
	if err != nil {
		t.Fatal(err)
	}
	positions, err := readLog(filepath.Join(dir, positionLogFile))
	if err != nil {
		t.Fatal(err)
	}

	// No gap (invariant 3).
	seen := map[int64]int{}
	for _, e := range ledger {
		seen[e.ID]++
	}
	for id := int64(1); id <= rows; id++ {
		if seen[id] == 0 {
			t.Errorf("row %d was never delivered (invariant 3)", id)
		}
	}
	for id := range seen {
		if id < 1 || id > rows {
			t.Errorf("row %d was delivered but never written", id)
		}
	}

	// Exact resume: nothing whose position was stored comes again.
	lastStored := map[int]string{}
	for _, p := range positions {
		lastStored[p.Run] = p.Pos
	}
	type rec struct {
		id int64
		op string
	}
	stored := map[rec]bool{}
	delivered := map[rec]int{}
	for _, e := range ledger {
		delivered[rec{e.ID, e.Op}]++
	}
	redelivered, run := 0, 0
	prefix := map[rec]bool{}
	reached := false
	flush := func() {
		if reached {
			for r := range prefix {
				stored[r] = true
			}
		}
		prefix, reached = map[rec]bool{}, false
	}
	for _, e := range ledger {
		if e.Run != run {
			flush()
			run = e.Run
		}
		r := rec{e.ID, e.Op}
		if stored[r] {
			t.Errorf("run %d delivered row %d (%s) again, although its position was stored by an earlier run", e.Run, e.ID, e.Op)
		}
		if delivered[r] > 1 {
			redelivered++
		}
		if !reached {
			prefix[r] = true
			if e.Pos == lastStored[e.Run] {
				reached = true
			}
		}
	}
	t.Logf("%d ledger entries for %d rows; %d entries are records delivered more than once (appended or acked, position not stored)", len(ledger), rows, redelivered)

	// Monotonic positions (invariant 2).
	inCDC := false
	var lastKey int64 = -1
	var lastCDC *common.CdcPosition
	for i, p := range positions {
		pos, err := common.ParseSDKPosition([]byte(p.Pos))
		if err != nil {
			t.Fatalf("position %d: %v", i, err)
		}
		switch {
		case pos.CdcPosition != nil:
			cp := pos.CdcPosition
			if lastCDC != nil && cdcLess(cp, lastCDC) {
				t.Errorf("position %d (run %d): CDC position %s:%d#%d is behind %s:%d#%d (invariant 2)",
					i, p.Run, cp.Name, cp.Pos, cp.Index, lastCDC.Name, lastCDC.Pos, lastCDC.Index)
			}
			lastCDC, inCDC = cp, true
		case pos.SnapshotPosition != nil:
			if inCDC {
				t.Errorf("position %d (run %d): a snapshot position after the pipeline was in CDC (invariant 2, #158)", i, p.Run)
			}
			tp, ok := pos.SnapshotPosition.Snapshots[table]
			if !ok || tp.SingleKey == nil {
				t.Fatalf("position %d: no single-key snapshot position for %s", i, table)
			}
			key, ok := tp.SingleKey.LastRead.(int64)
			if !ok {
				t.Fatalf("position %d: last_read is %T", i, tp.SingleKey.LastRead)
			}
			if key < lastKey {
				t.Errorf("position %d (run %d): snapshot key %d is behind %d (invariant 2)", i, p.Run, key, lastKey)
			}
			lastKey = key
		default:
			t.Errorf("position %d: neither snapshot nor CDC", i)
		}
	}
	if !inCDC {
		t.Error("the history never reached CDC")
	}
}

func cdcLess(a, b *common.CdcPosition) bool {
	if a.Name != b.Name {
		return a.Name < b.Name
	}
	if a.Pos != b.Pos {
		return a.Pos < b.Pos
	}
	return a.Index < b.Index
}
