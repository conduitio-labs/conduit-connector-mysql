// Copyright © 2024 Meroxa, Inc.
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
	"strconv"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

// benchUsers returns n distinct records that share one field set, so
// upsertRecords builds a single column run per Write. A batch re-upserts its
// own keys, keeping the table bounded at n rows.
func benchUsers(n int) []opencdc.Record {
	recs := make([]opencdc.Record, n)
	for i := range recs {
		metadata := opencdc.Metadata{}
		metadata.SetCollection("bench_users")
		recs[i] = opencdc.Record{
			Operation: opencdc.OperationCreate,
			Metadata:  metadata,
			Key:       opencdc.StructuredData{"id": int64(i)},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"id":         int64(i),
					"username":   "user-" + strconv.Itoa(i),
					"email":      "user-" + strconv.Itoa(i) + "@example.com",
					"created_at": "2024-01-01 00:00:00",
					"score":      float64(i % 1000),
					"active":     true,
				},
			},
		}
	}
	return recs
}

// newBenchDestination opens a real Destination against the test MySQL and
// creates the benchmark table, dropping it again when the benchmark is done so
// it does not leak into the integration tests' table detection.
func newBenchDestination(b *testing.B) *Destination {
	ctx := context.Background()
	setupDB, err := sqlx.Open("mysql", testutils.DSN)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if _, err := setupDB.ExecContext(context.Background(), `DROP TABLE IF EXISTS bench_users`); err != nil {
			b.Logf("failed to drop benchmark table: %v", err)
		}
		_ = setupDB.Close()
	})
	if _, err = setupDB.ExecContext(ctx, `DROP TABLE IF EXISTS bench_users`); err != nil {
		b.Fatal(err)
	}
	if _, err = setupDB.ExecContext(ctx, `CREATE TABLE bench_users (
		id BIGINT PRIMARY KEY,
		username VARCHAR(255),
		email VARCHAR(255),
		created_at VARCHAR(64),
		score DOUBLE,
		active TINYINT(1)
	)`); err != nil {
		b.Fatal(err)
	}

	d := &Destination{}
	cfg := map[string]string{"dsn": testutils.DSN}
	if err := sdk.Util.ParseConfig(ctx, cfg, d.Config(), Connector.NewSpecification().DestinationParams); err != nil {
		b.Fatal(err)
	}
	if err := d.Open(ctx); err != nil {
		b.Fatal(err)
	}
	return d
}

func benchmarkDestinationWrite(b *testing.B, batchSize int) {
	ctx := context.Background()
	d := newBenchDestination(b)
	b.Cleanup(func() { _ = d.Teardown(context.Background()) })

	recs := benchUsers(batchSize)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := d.Write(ctx, recs); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDestinationWrite1(b *testing.B)    { benchmarkDestinationWrite(b, 1) }
func BenchmarkDestinationWrite10(b *testing.B)   { benchmarkDestinationWrite(b, 10) }
func BenchmarkDestinationWrite100(b *testing.B)  { benchmarkDestinationWrite(b, 100) }
func BenchmarkDestinationWrite1000(b *testing.B) { benchmarkDestinationWrite(b, 1000) }
func BenchmarkDestinationWrite3000(b *testing.B) { benchmarkDestinationWrite(b, 3000) }
