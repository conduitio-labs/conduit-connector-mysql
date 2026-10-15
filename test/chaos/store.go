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
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	ledgerFile      = "ledger.jsonl"
	positionLogFile = "positions.jsonl"
	positionFile    = "position"
	parkedFile      = "parked"
	openedFile      = "opened"
)

// entry is one line of the ledger or of the position log.
type entry struct {
	Run int    `json:"run"`
	ID  int64  `json:"id,omitempty"` // ledger only
	Op  string `json:"op,omitempty"` // ledger only
	Pos string `json:"pos"`
}

// appendLog appends entries to a JSON-lines file and fsyncs it before
// returning, so an entry that was returned is on disk when the process is
// killed right after.
func appendLog(path string, entries []entry) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // harness temp dir
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	w := bufio.NewWriter(f)
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("marshal: %w", err)
		}
		_, _ = w.Write(b)
		_ = w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("fsync %s: %w", path, err)
	}
	return f.Close() //nolint:wrapcheck // close after a successful sync
}

// readLog reads a JSON-lines file. A torn last line (the process was killed
// while writing it, before the fsync returned) is ignored: its entry was never
// acknowledged as written.
func readLog(path string) ([]entry, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	var out []entry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var e entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			break // torn tail
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return out, nil
}

// storePosition replaces the position file atomically: temp file, fsync,
// rename, fsync the directory. A kill at any instant leaves either the old or
// the new position, never a torn one (invariant 5).
func storePosition(dir string, pos []byte) error {
	tmp, err := os.CreateTemp(dir, positionFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp position: %w", err)
	}
	if _, err := tmp.Write(pos); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp position: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync temp position: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp position: %w", err)
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, positionFile)); err != nil { //nolint:gosec // harness temp dir
		return fmt.Errorf("rename position: %w", err)
	}
	d, err := os.Open(dir) //nolint:gosec // harness temp dir
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	defer d.Close()
	return d.Sync() //nolint:wrapcheck // directory fsync
}

// loadPosition returns the stored position, or nil if none was stored.
func loadPosition(dir string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(dir, positionFile)) //nolint:gosec // harness temp dir
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read position: %w", err)
	}
	return b, nil
}
