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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/jmoiron/sqlx"
	"github.com/matryer/is"
)

// TestAcceptanceV2 runs the SDK acceptance suite v2 (revision pinned by the SDK
// version in go.mod) against a real MySQL server: test/docker-compose.yml, or
// the server MYSQL_TEST_DSN points at. Each case gets its own table, so cases
// cannot see each other's rows or binlog events.
//
// Write the machine-readable report with:
//
//	CONDUIT_ACCEPTANCE_REPORT=acceptance-report.json go test -race -run '^TestAcceptanceV2$' .
//
// Claims are only what this test proves. The README (Testing, Acceptance suite
// v2) says why each capability is or is not claimed.
func TestAcceptanceV2(t *testing.T) {
	srcCfg := map[string]string{
		"dsn":             testutils.DSN,
		"cdc.disableLogs": "true",
	}
	destCfg := map[string]string{
		"dsn": testutils.DSN,
	}

	driver := &acceptanceV2Driver{}
	driver.Config = sdk.ConfigurableAcceptanceTestDriverConfig{
		Connector:         decodingConnector(),
		SourceConfig:      srcCfg,
		DestinationConfig: destCfg,

		BeforeTest: func(t *testing.T) {
			driver.table = acceptanceTableName(t.Name())
			driver.createTable(t)
			// Table filters are unanchored regexes.
			srcCfg["tables"] = "^" + driver.table + "$"
		},
		AfterTest: func(t *testing.T) {
			driver.dropTable(t)
		},

		GenerateDataType: sdk.GenerateStructuredData,

		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,

		Capabilities: sdk.AcceptanceCapabilities{
			Source: sdk.SourceCapabilities{
				// Snapshot positions hold the last key read per table and
				// CDC positions the binlog event plus the row index inside
				// it, so a restart continues after the last acked record.
				ExactResume: true,
			},
			Destination: sdk.DestinationCapabilities{
				// Writes are INSERT ... ON DUPLICATE KEY UPDATE on the
				// table's primary key: a replayed record overwrites its row.
				// Holds for tables with a primary key, which is what this
				// test uses; a table without one appends.
				Idempotence: sdk.IdempotenceYes,
				// SchemaEvolution is deliberately not claimed. A batch whose
				// first record lacks a field that a later record has writes
				// the later record without that field (the column list comes
				// from the first record), so "reject" does not hold in
				// general even though DST-07 passes: DST-07 sends the new
				// field in a batch of its own.
			},
		},
	}

	sdk.AcceptanceTestV2(t, driver)
}

func acceptanceTableName(testName string) string {
	sum := sha256.Sum256([]byte(testName))
	return "acc_" + hex.EncodeToString(sum[:6])
}

// acceptanceV2Driver maps suite records onto a fixed four-column table. name is
// LONGTEXT so that the large record cases can carry 1 MiB in it.
type acceptanceV2Driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	table string
}

var (
	_ sdk.AcceptanceReadAllDriver        = (*acceptanceV2Driver)(nil)
	_ sdk.AcceptanceLargeRecordGenerator = (*acceptanceV2Driver)(nil)
)

// acceptanceNextID makes record ids unique across the whole run, so that
// generated records never collide on the primary key.
var acceptanceNextID = func() *atomic.Int64 {
	var v atomic.Int64
	v.Store(rand.Int63n(1 << 40))
	return &v
}()

// withDB runs fn with a short-lived pool that is closed before returning, so
// the suite's goroutine-leak checks only see the connector's own goroutines.
func (d *acceptanceV2Driver) withDB(t *testing.T, fn func(db *sqlx.DB)) {
	t.Helper()
	db, err := sqlx.Open("mysql", testutils.DSN)
	is.New(t).NoErr(err)
	defer db.Close()
	fn(db)
}

func (d *acceptanceV2Driver) createTable(t *testing.T) {
	is := is.New(t)
	d.withDB(t, func(db *sqlx.DB) {
		_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+d.table)
		is.NoErr(err)
		_, err = db.ExecContext(t.Context(), fmt.Sprintf(`CREATE TABLE %s (
			id     BIGINT   NOT NULL PRIMARY KEY,
			name   LONGTEXT NOT NULL,
			amount DOUBLE   NOT NULL,
			qty    INT      NOT NULL
		)`, d.table))
		is.NoErr(err)
	})
}

func (d *acceptanceV2Driver) dropTable(t *testing.T) {
	d.withDB(t, func(db *sqlx.DB) {
		_, err := db.ExecContext(t.Context(), "DROP TABLE IF EXISTS "+d.table)
		is.New(t).NoErr(err)
	})
}

func (d *acceptanceV2Driver) record(op opencdc.Operation, name string) opencdc.Record {
	id := acceptanceNextID.Add(1)
	meta := opencdc.Metadata{}
	meta.SetCollection(d.table)
	return opencdc.Record{
		Position:  opencdc.Position(fmt.Sprintf("pos-%d", id)),
		Operation: op,
		Metadata:  meta,
		Key:       opencdc.StructuredData{"id": id},
		Payload: opencdc.Change{After: opencdc.StructuredData{
			"id":     id,
			"name":   name,
			"amount": float64(rand.Intn(1_000_000)) / 100,
			"qty":    rand.Intn(1000),
		}},
	}
}

func (d *acceptanceV2Driver) GenerateRecord(_ *testing.T, op opencdc.Operation) opencdc.Record {
	return d.record(op, fmt.Sprintf("name-%d", rand.Int63()))
}

// GenerateLargeRecord puts the padding into the LONGTEXT column; the table has
// no column for the suite's default padding field.
func (d *acceptanceV2Driver) GenerateLargeRecord(_ *testing.T, op opencdc.Operation, size int) opencdc.Record {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var b strings.Builder
	b.Grow(size)
	for range size {
		b.WriteByte(letters[rand.Intn(len(letters))])
	}
	return d.record(op, b.String())
}

// WriteToSource inserts the records in multi-row statements of up to 250 rows,
// each its own transaction, so change capture sees both single-row and
// multi-row binlog events.
func (d *acceptanceV2Driver) WriteToSource(t *testing.T, recs []opencdc.Record) []opencdc.Record {
	is := is.New(t)
	const chunk = 250
	d.withDB(t, func(db *sqlx.DB) {
		for start := 0; start < len(recs); start += chunk {
			end := min(start+chunk, len(recs))
			var (
				q    strings.Builder
				args []any
			)
			q.WriteString("INSERT INTO " + d.table + " (id, name, amount, qty) VALUES ")
			for i, rec := range recs[start:end] {
				if i > 0 {
					q.WriteString(", ")
				}
				q.WriteString("(?, ?, ?, ?)")
				p := rec.Payload.After.(opencdc.StructuredData)
				args = append(args, p["id"], p["name"], p["amount"], p["qty"])
			}
			_, err := db.ExecContext(t.Context(), q.String(), args...)
			is.NoErr(err)
		}
	})
	return recs
}

// ReadFromDestination returns every row of the table. A primary-key table
// cannot hold duplicates, so this is also everything stored.
func (d *acceptanceV2Driver) ReadFromDestination(t *testing.T, _ []opencdc.Record) []opencdc.Record {
	return d.ReadAllFromDestination(t)
}

func (d *acceptanceV2Driver) ReadAllFromDestination(t *testing.T) []opencdc.Record {
	is := is.New(t)

	var out []opencdc.Record
	d.withDB(t, func(db *sqlx.DB) {
		rows, err := db.QueryxContext(t.Context(), "SELECT id, name, amount, qty FROM "+d.table+" ORDER BY id")
		is.NoErr(err)
		defer rows.Close()

		for rows.Next() {
			var (
				id     int64
				name   string
				amount float64
				qty    int
			)
			is.NoErr(rows.Scan(&id, &name, &amount, &qty))

			meta := opencdc.Metadata{}
			meta.SetCollection(d.table)
			out = append(out, opencdc.Record{
				Operation: opencdc.OperationCreate,
				Metadata:  meta,
				Key:       opencdc.StructuredData{"id": id},
				Payload: opencdc.Change{After: opencdc.StructuredData{
					"id": id, "name": name, "amount": amount, "qty": qty,
				}},
			})
		}
		is.NoErr(rows.Err())
	})
	return out
}

// decodingConnector returns the connector with its source wrapped so that the
// records it hands to the suite are decoded from Avro.
//
// The suite identifies a record read from a source by its payload bytes. This
// source registers a schema for every table and the SDK's encoding middleware
// turns the payload into Avro bytes, which the driver cannot predict when it
// writes a row. The wrapper decodes key and payload with the schema named in
// the record's metadata, which is what Conduit's destination middleware does.
// It sits outside the connector and its SDK middleware, does not touch
// positions, acks or the order of records, and fails the read if a record
// cannot be decoded, so it can hide no connector behavior other than the
// encoding itself.
func decodingConnector() sdk.Connector {
	c := Connector
	c.NewSource = func() sdk.Source { return &decodingSource{Source: NewSource()} }
	return c
}

type decodingSource struct {
	sdk.Source
}

func (s *decodingSource) ReadN(ctx context.Context, n int) ([]opencdc.Record, error) {
	recs, err := s.Source.ReadN(ctx, n)
	if err != nil {
		return recs, err //nolint:wrapcheck // the suite matches on the connector's errors
	}
	for i := range recs {
		if err := decodeRecord(ctx, &recs[i]); err != nil {
			return nil, fmt.Errorf("decode record %d: %w", i, err)
		}
	}
	return recs, nil
}

func decodeRecord(ctx context.Context, rec *opencdc.Record) error {
	if subj, err := rec.Metadata.GetKeySchemaSubject(); err == nil {
		ver, err := rec.Metadata.GetKeySchemaVersion()
		if err != nil {
			return fmt.Errorf("key schema version: %w", err)
		}
		if rec.Key, err = decodeData(ctx, rec.Key, subj, ver); err != nil {
			return fmt.Errorf("key: %w", err)
		}
	}
	subj, err := rec.Metadata.GetPayloadSchemaSubject()
	if err != nil {
		return fmt.Errorf("record has no payload schema: %w", err)
	}
	ver, err := rec.Metadata.GetPayloadSchemaVersion()
	if err != nil {
		return fmt.Errorf("payload schema version: %w", err)
	}
	if rec.Payload.Before, err = decodeData(ctx, rec.Payload.Before, subj, ver); err != nil {
		return fmt.Errorf("payload.before: %w", err)
	}
	if rec.Payload.After, err = decodeData(ctx, rec.Payload.After, subj, ver); err != nil {
		return fmt.Errorf("payload.after: %w", err)
	}
	return nil
}

func decodeData(ctx context.Context, data opencdc.Data, subject string, version int) (opencdc.Data, error) {
	raw, ok := data.(opencdc.RawData)
	if !ok {
		return data, nil // nil or already structured
	}
	sch, err := schema.Get(ctx, subject, version)
	if err != nil {
		return nil, fmt.Errorf("get schema %s:%d: %w", subject, version, err)
	}
	var out opencdc.StructuredData
	if err := sch.Unmarshal(raw.Bytes(), &out); err != nil {
		return nil, fmt.Errorf("unmarshal with schema %s:%d: %w", subject, version, err)
	}
	return out, nil
}
