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
	"errors"
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

// maxBaseDefects is how many sequences may be skipped because a child built
// from the base crashed by itself before it parked. A base that crashes
// more often than that is too broken to test an upgrade from.
//
// Until a release contains #228, the base is 4676f45, whose source shares
// record metadata between goroutines: about one sequence in three dies with
// "fatal error: concurrent map read and map write" in the child. That is a
// defect of the base, not of this build; #228 fixed it. Once the base is a
// release that contains #228, no base child should crash, the allowance
// is dead weight, and it should be removed (set it to 0).
const maxBaseDefects = 2

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
// restart, resume is also exact, each run starts right after the position it
// opened at, and positions are monotonic (verify), unless the release itself
// went back to the snapshot after CDC (#158, fixed after v0.3.0): then this
// build correctly continues the snapshot the release stored, rows come again,
// and only the no-gap check applies (logged, and a GitHub warning). If that
// happens to every exact sequence, the test fails: with no exact check left,
// a source that skips rows on resume would pass. The
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
	exact, fellBack, baseDefects := 0, 0, 0
	for i, s := range seqs {
		t.Run(s.name, func(t *testing.T) {
			defer func() {
				if s.exact && !t.Skipped() {
					exact++ // the exact checks that actually ran
				}
			}()
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

			// wait is h.waitMarker, except that a base child which exits by
			// itself ends the sequence as a base defect (see maxBaseDefects).
			wait := func(c *childProc, file, what string, base bool) {
				t.Helper()
				if !base {
					h.waitMarker(c, file, what)
					return
				}
				err := h.awaitMarker(c, file, what)
				var exited *childExitError
				switch {
				case err == nil:
				case errors.As(err, &exited):
					baseDefects++
					msg := fmt.Sprintf("the base child exited by itself before it did %s (base defect %d, at most %d allowed): %v\n%s",
						what, baseDefects, maxBaseDefects, exited.err, exited.log)
					t.Log(msg)
					if os.Getenv("GITHUB_ACTIONS") == "true" {
						fmt.Printf("::warning::%s: the base child crashed (base defect %d of at most %d); sequence skipped\n",
							t.Name(), baseDefects, maxBaseDefects)
					}
					t.Skip("the base child crashed by itself; counted as a base defect")
				default:
					t.Fatal(err)
				}
			}

			// Into change capture: row 1 is written after Open.
			c := h.startBinary(binFor(s.run0), "store_id:1")
			wait(c, openedFile, "open its source", s.run0)
			h.insertRange(1, 1)
			wait(c, parkedFile, "park", s.run0)
			h.kill(c)

			h.insertRange(2, 9)
			h.insertRange(10, 10)
			h.insertRange(11, 13)

			for _, st := range s.steps {
				c := h.startBinary(binFor(st.base), "store_id:"+strconv.FormatInt(st.id, 10))
				wait(c, parkedFile, "park", st.base)
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
					fellBack++
					t.Log("the release went back to the snapshot after CDC (#158); checking for gaps only")
					if os.Getenv("GITHUB_ACTIONS") == "true" {
						fmt.Printf("::warning::%s: the base went back to the snapshot after CDC (#158); exact checks skipped\n", t.Name())
					}
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
			t.Logf("%d rows delivered more than once; allowed here, because a release resuming inside an event replays it from its first row", dups)
		})
	}
	if baseDefects > maxBaseDefects {
		t.Errorf("%d of %d sequences were skipped because the base child crashed by itself, more than the %d allowed. "+
			"The base is too broken to test an upgrade from; use a release that contains #228 as the base",
			baseDefects, len(seqs), maxBaseDefects)
	}
	if exact > 0 && fellBack == exact {
		t.Errorf("all %d exact sequences fell back to the gap check, because the base goes back to the snapshot after CDC "+
			"(#158): nothing here would catch a source that skips rows on resume. Use a base that has the #158 fix (f86aea9)", exact)
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
