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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
)

// envBaseChild names a build of cmd/chaos-child from the last release of the
// connector. The nightly workflow (.github/workflows/chaos-upgrade.yml) builds
// it; without it the test is skipped.
const envBaseChild = "MYSQL_CHAOS_BASE_CHILD"

// step is one child run: which build, and where it parks to be killed.
type step struct {
	base bool  // the released build; otherwise this one
	id   int64 // park right after storing the position of row id
}

// TestSIGKILL_UpgradeDowngrade runs the released source and this one, as real
// processes, against the same stored positions, killing each right after it
// stored the position of a chosen row (one record per batch, so a kill can
// land after any row). A final run of this build must deliver every row.
//
// Workload: row 1 is read by change capture, then rows 2-9 in one statement
// (one rows event), row 10 alone, rows 11-13 in one statement.
//
// Checked for every sequence: no gap (invariant 3) and no row that was never
// written. For an upgrade from positions the release wrote with a single
// restart, resume is also exact and positions are monotonic (verify), unless
// the release itself went back to the snapshot after CDC (#158, fixed after
// v0.3.0): then this build correctly continues the snapshot the release
// stored, rows come again, and only the no-gap check applies (logged). The
// release replays a multi-row event from its first row when it resumes inside
// it, and the positions it writes after that cannot say which rows came
// before; those sequences may deliver rows again, which is counted, not
// failed.
func TestSIGKILL_UpgradeDowngrade(t *testing.T) {
	baseChild := os.Getenv(envBaseChild)
	if baseChild == "" {
		t.Skip(envBaseChild + " is not set; the nightly chaos-upgrade workflow builds it")
	}
	db, err := sql.Open("mysql", testDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	type sequence struct {
		name  string
		run0  bool // the run that stores row 1's position is the release
		steps []step
		exact bool
	}
	var seqs []sequence
	for _, a := range []int64{2, 5, 9, 10, 12} {
		seqs = append(seqs, sequence{fmt.Sprintf("upgrade/release-at-%d", a), true, []step{{true, a}}, true})
	}
	for _, ab := range [][2]int64{{3, 6}, {4, 9}, {5, 11}} {
		a, b := ab[0], ab[1]
		seqs = append(seqs,
			sequence{fmt.Sprintf("upgrade-after-release-resumed/%d-%d", a, b), true, []step{{true, a}, {true, b}}, false},
			sequence{fmt.Sprintf("downgrade/this-%d-release-%d", a, b), false, []step{{false, a}, {true, b}}, false},
		)
	}

	const total = 13
	for i, s := range seqs {
		t.Run(s.name, func(t *testing.T) {
			h := &harness{t: t, dir: t.TempDir(), table: "chaos_updown_" + strconv.Itoa(i), db: db, batch: "1"}
			h.exec("DROP TABLE IF EXISTS " + h.table)
			h.exec("CREATE TABLE " + h.table + " (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(32) NOT NULL)")
			t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+h.table) })

			baseRuns := map[int]bool{}
			binFor := func(base bool) string {
				if base {
					baseRuns[h.run+1] = true // the run startBinary is about to start
					return baseChild
				}
				return ""
			}

			// Into change capture: row 1 is written after Open.
			c := h.startBinary(binFor(s.run0), "store_id:1")
			h.waitOpened(c)
			h.insertRange(1, 1)
			h.waitParked(c)
			h.kill(c)

			h.insertRange(2, 9)
			h.insertRange(10, 10)
			h.insertRange(11, 13)

			for _, st := range s.steps {
				c := h.startBinary(binFor(st.base), "store_id:"+strconv.FormatInt(st.id, 10))
				h.waitParked(c)
				h.kill(c)
			}

			c = h.start("")
			h.waitLedgerHas(c, total)
			if err := c.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-c.done:
				if err != nil {
					t.Fatalf("final run did not stop cleanly on SIGTERM: %v\n%s", err, c.log.String())
				}
			case <-time.After(30 * time.Second):
				t.Fatalf("final run did not stop within 30s of SIGTERM\n%s", c.log.String())
			}

			if s.exact {
				if releaseResnapshotted(t, h, baseRuns) {
					t.Log("the release went back to the snapshot after CDC (#158); checking for gaps only")
				} else {
					verify(t, h.dir, h.table, total)
					return
				}
			}
			dups := 0
			seen := map[int64]int{}
			for _, e := range h.ledger() {
				seen[e.ID]++
			}
			for id := int64(1); id <= total; id++ {
				if seen[id] == 0 {
					t.Errorf("row %d was never delivered (invariant 3)", id)
				}
			}
			for id, n := range seen {
				if id < 1 || id > total {
					t.Errorf("row %d was delivered but never written", id)
				}
				dups += n - 1
			}
			t.Logf("%d rows delivered more than once (the release's replay from the first row)", dups)
		})
	}
}

// releaseResnapshotted reports whether a run of the release stored a snapshot
// position after a CDC position had been stored.
func releaseResnapshotted(t *testing.T, h *harness, baseRuns map[int]bool) bool {
	t.Helper()
	positions, err := readLog(filepath.Join(h.dir, positionLogFile))
	if err != nil {
		t.Fatal(err)
	}
	inCDC := false
	for _, p := range positions {
		pos, err := common.ParseSDKPosition([]byte(p.Pos))
		if err != nil {
			t.Fatalf("position of run %d: %v", p.Run, err)
		}
		switch {
		case pos.CdcPosition != nil:
			inCDC = true
		case pos.SnapshotPosition != nil && inCDC && baseRuns[p.Run]:
			return true
		}
	}
	return false
}
