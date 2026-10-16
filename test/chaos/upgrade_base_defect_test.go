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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeChild writes a shell script that stands in for a build of
// cmd/chaos-child.
func fakeChild(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-child")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil { //nolint:gosec // a test script
		t.Fatal(err)
	}
	return path
}

// A child that exits by itself before it parks (the base build's concurrent
// map crash) is reported as a *childExitError with its output, so the upgrade
// test can count it as a base defect instead of failing on it. Needs no
// database.
func TestAwaitMarkerReportsAChildThatExits(t *testing.T) {
	h := &harness{t: t, dir: t.TempDir()}
	c := h.startBinary(fakeChild(t, "echo 'fatal error: concurrent map read and map write'; exit 2"), "")

	err := h.awaitMarker(c, parkedFile, "park")

	var exited *childExitError
	if !errors.As(err, &exited) {
		t.Fatalf("want a *childExitError, got %v", err)
	}
	if !strings.Contains(exited.log, "concurrent map read and map write") || exited.run != 1 {
		t.Fatalf("the error does not carry the child's run and output: %+v", exited)
	}
}

// A child that parks is not an error.
func TestAwaitMarkerReturnsWhenTheChildParks(t *testing.T) {
	h := &harness{t: t, dir: t.TempDir()}
	c := h.startBinary(fakeChild(t, `printf 1 > "$`+envDir+`/`+parkedFile+`"; exec sleep 60`), "")

	if err := h.awaitMarker(c, parkedFile, "park"); err != nil {
		t.Fatal(err)
	}
	h.kill(c)
}
