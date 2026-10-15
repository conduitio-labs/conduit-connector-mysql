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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	mysql "github.com/conduitio-labs/conduit-connector-mysql"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/rs/zerolog"
)

// Environment of a child process. The parent passes these and nothing else
// besides PATH, HOME and TMPDIR.
const (
	envChild = "MYSQL_CHAOS_CHILD"
	envDir   = "MYSQL_CHAOS_DIR"
	envRun   = "MYSQL_CHAOS_RUN"
	envTable = "MYSQL_CHAOS_TABLE"
	envPark  = "MYSQL_CHAOS_PARK"
	envDSN   = "MYSQL_TEST_DSN"
)

// parkPoint is where a child stops and waits to be killed:
// "append:N" after the batch holding its Nth record is in the ledger, before
// any of that batch is acked; "ack:N" right after its Nth ack, before the
// position is stored; "store_id:X" right after storing the position of the
// batch that holds row X. Counts are per child.
type parkPoint struct {
	kind string
	n    int64
}

func parsePark(s string) (parkPoint, error) {
	if s == "" {
		return parkPoint{}, nil
	}
	kind, num, ok := strings.Cut(s, ":")
	if !ok {
		return parkPoint{}, fmt.Errorf("bad park point %q", s)
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return parkPoint{}, fmt.Errorf("bad park point %q: %w", s, err)
	}
	switch kind {
	case "append", "ack", "store_id":
	default:
		return parkPoint{}, fmt.Errorf("bad park point kind %q", kind)
	}
	return parkPoint{kind: kind, n: n}, nil
}

// runChild is the child process: a real source, the ledger and the position
// file, in the engine's order. It returns the process exit code.
func runChild() int {
	if err := child(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos child: %v\n", err)
		return 2
	}
	return 0
}

func child() error {
	dir := os.Getenv(envDir)
	run, err := strconv.Atoi(os.Getenv(envRun))
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	park, err := parsePark(os.Getenv(envPark))
	if err != nil {
		return err
	}

	watchParent()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	logger := zerolog.New(os.Stderr).Level(zerolog.WarnLevel).With().Timestamp().Logger()
	ctx = logger.WithContext(ctx)

	src := mysql.NewSource()
	cfg := map[string]string{
		"dsn":                os.Getenv(envDSN),
		"tables":             "^" + os.Getenv(envTable) + "$",
		"snapshot.fetchSize": "50",
		"cdc.disableLogs":    "true",
		// Small batches, so kills land between stored positions inside the
		// snapshot and inside one binlog event.
		"sdk.batch.size":  "20",
		"sdk.batch.delay": "100ms",
	}
	if err := sdk.Util.ParseConfig(ctx, cfg, src.Config(), mysql.Connector.NewSpecification().SourceParams); err != nil {
		return fmt.Errorf("configure: %w", err)
	}
	pos, err := loadPosition(dir)
	if err != nil {
		return err
	}
	// Record where this run starts, before opening: verify checks that the
	// run's first record is the one right after it.
	if err := appendLog(filepath.Join(dir, opensLogFile), []entry{{Run: run, Pos: string(pos)}}); err != nil {
		return err
	}
	if err := src.Open(ctx, pos); err != nil {
		return fmt.Errorf("open: %w", err)
	}
	// Tell the parent that the source is open: rows written from now on are
	// read by change capture, not by a resumed snapshot.
	//nolint:gosec // dir is the parent harness's temp dir
	if err := os.WriteFile(filepath.Join(dir, openedFile), []byte(strconv.Itoa(run)), 0o600); err != nil {
		return fmt.Errorf("write opened marker: %w", err)
	}

	var appended, acked int64
	for {
		recs, err := src.ReadN(ctx, 100)
		if ctx.Err() != nil {
			// SIGTERM: graceful stop (invariant 7). Everything read was
			// acked and stored, or will be read again.
			if err := src.Teardown(context.WithoutCancel(ctx)); err != nil {
				return fmt.Errorf("teardown: %w", err)
			}
			return nil
		}
		if errors.Is(err, sdk.ErrBackoffRetry) || (err == nil && len(recs) == 0) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		// 1. durable downstream
		entries := make([]entry, len(recs))
		holds := false
		for i, r := range recs {
			id, err := recordID(ctx, r)
			if err != nil {
				return err
			}
			entries[i] = entry{Run: run, ID: id, Op: r.Operation.String(), Pos: string(r.Position)}
			if park.kind == "store_id" && id == park.n {
				holds = true
			}
		}
		if err := appendLog(filepath.Join(dir, ledgerFile), entries); err != nil {
			return err
		}
		appended += int64(len(recs))
		if park.kind == "append" && appended >= park.n {
			parkForever(dir, run)
		}

		// 2. ack
		for _, r := range recs {
			if err := src.Ack(ctx, r.Position); err != nil {
				return fmt.Errorf("ack: %w", err)
			}
			acked++
			if park.kind == "ack" && acked == park.n {
				parkForever(dir, run)
			}
		}

		// 3. position: the file first, then the audit log, so the log never
		// holds a position the next run did not start from or after.
		last := recs[len(recs)-1].Position
		if err := storePosition(dir, last); err != nil {
			return err
		}
		if err := appendLog(filepath.Join(dir, positionLogFile), []entry{{Run: run, Pos: string(last)}}); err != nil {
			return err
		}
		if holds {
			parkForever(dir, run)
		}
	}
}

// recordID decodes the record key with the schema in its metadata and returns
// the id column.
func recordID(ctx context.Context, r opencdc.Record) (int64, error) {
	subject, err := r.Metadata.GetKeySchemaSubject()
	if err != nil {
		return 0, fmt.Errorf("key schema subject: %w", err)
	}
	version, err := r.Metadata.GetKeySchemaVersion()
	if err != nil {
		return 0, fmt.Errorf("key schema version: %w", err)
	}
	sch, err := schema.Get(ctx, subject, version)
	if err != nil {
		return 0, fmt.Errorf("get key schema: %w", err)
	}
	var key map[string]any
	if err := sch.Unmarshal(r.Key.Bytes(), &key); err != nil {
		return 0, fmt.Errorf("decode key: %w", err)
	}
	id, ok := key["id"].(int64)
	if !ok {
		return 0, fmt.Errorf("key id is %T, want int64", key["id"])
	}
	return id, nil
}

// parkForever tells the parent where the child stopped and blocks until it is
// killed (or watchParent ends the process).
func parkForever(dir string, run int) {
	_ = os.WriteFile(filepath.Join(dir, parkedFile), []byte(strconv.Itoa(run)), 0o600) //nolint:gosec // harness temp dir
	for {
		time.Sleep(time.Hour)
	}
}

// childLifetime bounds a child, parked or not, in case nobody kills it.
const childLifetime = 15 * time.Minute

// watchParent ends the child when the parent test process is gone (the child
// is re-parented, so its parent pid changes) or after childLifetime, so a
// child never outlives a killed or crashed test run.
func watchParent() {
	parent := os.Getppid()
	deadline := time.Now().Add(childLifetime)
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			if os.Getppid() != parent {
				fmt.Fprintln(os.Stderr, "chaos child: parent is gone, exiting")
				os.Exit(3)
			}
			if time.Now().After(deadline) {
				fmt.Fprintf(os.Stderr, "chaos child: still running after %s, exiting\n", childLifetime)
				os.Exit(4)
			}
		}
	}()
}
