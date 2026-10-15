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
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/go-mysql-org/go-mysql/canal"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/matryer/is"
)

// logPosRecorder records the end offset (Header.LogPos) of every rows event
// canal hands over.
type logPosRecorder struct {
	canal.DummyEventHandler
	logPos chan uint32
}

func (r *logPosRecorder) OnRow(e *canal.RowsEvent) error {
	r.logPos <- e.Header.LogPos
	return nil
}

func (r *logPosRecorder) String() string { return "logPosRecorder" }

// Canary for the dependency: the compressed resume (#228) tells the rows of a
// compressed transaction (binlog_transaction_compression=ON) by their offset
// being 0, because go-mysql hands over the events inside a
// TRANSACTION_PAYLOAD_EVENT with LogPos 0 (cdcEventHandler.onRows, OnRow).
//
// go-mysql-org/go-mysql ace1fb4 (in v1.16.0, "make compressed transaction
// payloads checkpointable") stamps those events with the offset of the
// payload's start instead. Then every row event of a compressed transaction
// looks like an ordinary event at one shared offset, and a resume from inside
// the transaction drops or repeats rows (invariant 3). This test fails when a
// go-mysql upgrade changes the offset, before the resume tests do, and names
// the reason.
func TestCanalDeliversCompressedSubEventsAtOffsetZero(t *testing.T) {
	ctx := testutils.TestContext(t)
	is := is.New(t)
	db := testutils.NewDB(t)
	testutils.CreateUserTable(is, db)
	exec := newSession(ctx, t, is)

	config, err := mysql.ParseDSN(testutils.DSN)
	is.NoErr(err)
	c, err := common.NewCanal(ctx, common.CanalConfig{Config: config, Tables: []string{"users"}, DisableLogging: true})
	is.NoErr(err)
	rec := &logPosRecorder{logPos: make(chan uint32, 16)}
	c.SetEventHandler(rec)
	from, err := c.GetMasterPos()
	is.NoErr(err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.RunFrom(gomysql.Position{Name: from.Name, Pos: from.Pos})
	}()
	defer func() {
		c.Close()
		<-done
	}()

	next := func() uint32 {
		select {
		case p := <-rec.logPos:
			return p
		case <-time.After(15 * time.Second):
			t.Fatal("no rows event within 15s")
			return 0
		}
	}

	exec.compression(false)
	exec.insertUsers(1, 1)
	exec.compression(true)
	exec.insertUsers(2, 2)

	if plain := next(); plain == 0 {
		t.Fatal("a rows event outside a compressed transaction has offset 0: go-mysql changed its offsets")
	}
	if compressed := next(); compressed != 0 {
		t.Fatalf("a rows event inside a compressed transaction has offset %d, not 0. go-mysql now stamps them "+
			"(upstream ace1fb4, in v1.16.0): the compressed resume (cdcEventHandler.onRows) tells such events "+
			"by offset 0, so it must be reworked before go-mysql is upgraded; see the replace comment in go.mod",
			compressed)
	}
}
