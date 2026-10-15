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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/iskorotkov/avro/v2"
)

// This file pins the Avro output of the MySQL schema mapper across the codec
// swap from github.com/hamba/avro/v2 (archived, three unfixed decoder
// advisories) to github.com/iskorotkov/avro/v2.
//
// Why it matters (data path): createSchema registers recordSchema.String()
// with the Conduit schema registry and the SDK encodes every record against
// it. If the new codec printed a different string for the same table, every
// existing pipeline would register a new schema version on upgrade; if it
// encoded a row to different bytes, records written before and after the
// upgrade would not be comparable (invariant 6).
//
// testdata/hamba_schema_fixtures.json was produced by running compatCases()
// against the mapper built on hamba/avro v2.31.0. To ADD a case, build the
// tree against hamba (swap the import back), run with HAMBA_FIXTURE_GEN=1 and
// commit only the new entries. Never regenerate existing entries with the new
// codec: they are the frozen record of what the old library produced.
//
// Codec status when this was written (checked 2026-10-14, per the Avro codec
// design doc): hamba/avro archived; iskorotkov/avro not archived, last push
// 2026-10-09, latest release v2.34.0 (2026-08-18).

type compatCase struct {
	Name string `json:"name"`
	// Cols are the mapper's column types, in schema order.
	Cols []string `json:"cols"`
}

type compatFixture struct {
	compatCase
	Schema string `json:"schema"`
	// Rows maps "snapshot" and "cdc" to the Avro binary encoding (hex) of the
	// row, or "ERR" when the connector's value cannot be encoded under the
	// schema (recorded so a change in that behavior is caught too).
	Rows map[string]string `json:"rows"`
	// Decoded maps the same keys to the exact-typed (%#v) decoding of the
	// hamba-encoded bytes.
	Decoded map[string]string `json:"decoded"`
}

type compatFixtureFile struct {
	Schemas []compatFixture `json:"schemas"`
}

// compatCol is one column of the matrix: how the mapper types it and what the
// two upstream paths hand to formatValue (database/sql in snapshot mode,
// canal in CDC mode).
type compatCol struct {
	col      *avroNamedType
	snapshot any
	cdc      any
}

// compatCols maps every entry of sqlColtypeToAvroTypeMap (snapshot, from
// database/sql column types) and mysqlschemaTypeToAvroTypeMap (CDC, from
// go-mysql table schema) plus the raw-type overrides in
// mysqlSchemaToAvroCol, so a new mapping without a fixture fails.
func compatCols(t *testing.T) map[string]compatCol {
	t.Helper()
	vals := map[string][2]any{
		"TINYINT": {int64(-7), int8(-7)}, "SMALLINT": {int64(-300), int16(-300)},
		"MEDIUMINT": {int64(70000), int(70000)}, "INT": {int64(-2147483648), int(-2147483648)},
		"BIGINT":           {int64(9223372036854775807), int64(9223372036854775807)},
		"UNSIGNED TINYINT": {uint8(255), uint8(255)}, "UNSIGNED SMALLINT": {uint16(65535), uint16(65535)},
		"UNSIGNED MEDIUMINT": {int64(16777215), int(16777215)},
		"UNSIGNED INT":       {uint32(4294967295), uint64(4294967295)},
		"UNSIGNED BIGINT":    {uint64(9223372036854775807), uint64(9223372036854775807)},
		"FLOAT":              {float32(1.5), float32(1.5)},
		"DECIMAL":            {[]byte("12.50"), "12.50"}, "NUMERIC": {[]byte("-0.125"), "-0.125"},
		"DOUBLE": {float64(3.14159), float64(3.14159)},
		"BIT":    {[]byte{0x05}, int64(5)},
		"CHAR":   {[]uint8("héllo"), "héllo"}, "VARCHAR": {[]uint8("foo bar"), "foo bar"},
		"TINYTEXT": {[]uint8("t"), "t"}, "TEXT": {[]uint8("text"), "text"},
		"MEDIUMTEXT": {[]uint8("mt"), "mt"}, "LONGTEXT": {[]uint8("lt"), "lt"},
		"BINARY": {[]byte{0, 1, 2}, "\x00\x01\x02"}, "VARBINARY": {[]byte{3, 4}, "\x03\x04"},
		"TINYBLOB": {[]byte{5}, "\x05"}, "BLOB": {[]byte{6}, "\x06"},
		"MEDIUMBLOB": {[]byte{7}, "\x07"}, "LONGBLOB": {[]byte{8}, "\x08"},
		"DATE": {"2024-01-02", "2024-01-02"}, "TIME": {"03:04:05", "03:04:05"},
		"DATETIME": {"2024-01-02 03:04:05", "2024-01-02 03:04:05"}, "TIMESTAMP": {"2024-01-02 03:04:05", "2024-01-02 03:04:05"},
		"YEAR": {int64(2024), uint16(2024)},
		"ENUM": {[]uint8("a"), "a"}, "SET": {[]uint8("a,b"), "a,b"}, "JSON": {[]uint8(`{"a":[1,2]}`), `{"a":[1,2]}`},
	}
	out := map[string]compatCol{}
	for name, at := range sqlColtypeToAvroTypeMap {
		v, ok := vals[name]
		if !ok {
			t.Fatalf("sqlColtypeToAvroTypeMap has %q: add it to compatCols and the fixtures", name)
		}
		out["sql:"+name] = compatCol{col: &avroNamedType{avroType: at, Name: "c_" + sanitize(name)}, snapshot: v[0], cdc: v[1]}
	}

	// CDC path: go-mysql table columns, including the RawType-driven overrides.
	canal := []struct {
		col mysqlschema.TableColumn
		v   [2]any
	}{
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_NUMBER, RawType: "tinyint(4)"}, vals["TINYINT"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_NUMBER, RawType: "int(11)"}, vals["INT"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_NUMBER, RawType: "int(10) unsigned"}, vals["UNSIGNED INT"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_NUMBER, RawType: "bigint(20)"}, vals["BIGINT"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_NUMBER, RawType: "bigint(20) unsigned"}, vals["UNSIGNED BIGINT"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_MEDIUM_INT, RawType: "mediumint"}, vals["MEDIUMINT"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_FLOAT, RawType: "float"}, vals["FLOAT"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_FLOAT, RawType: "double"}, vals["DOUBLE"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_DECIMAL, RawType: "decimal(10,2)"}, vals["DECIMAL"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_STRING, RawType: "varchar(20)"}, vals["VARCHAR"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_ENUM, RawType: "enum('a','b')"}, vals["ENUM"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_SET, RawType: "set('a','b')"}, vals["SET"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_BINARY, RawType: "varbinary(8)"}, vals["VARBINARY"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_BINARY, RawType: "tinyblob"}, vals["TINYBLOB"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_BINARY, RawType: "blob"}, vals["BLOB"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_BINARY, RawType: "mediumblob"}, vals["MEDIUMBLOB"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_BINARY, RawType: "longblob"}, vals["LONGBLOB"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_BIT, RawType: "bit(8)"}, vals["BIT"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_DATETIME, RawType: "datetime"}, vals["DATETIME"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_TIMESTAMP, RawType: "timestamp"}, vals["TIMESTAMP"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_DATE, RawType: "date"}, vals["DATE"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_TIME, RawType: "time"}, vals["TIME"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_JSON, RawType: "json"}, vals["JSON"]},
		{mysqlschema.TableColumn{Type: mysqlschema.TYPE_POINT, RawType: "point"}, [2]any{"POINT(1 2)", "POINT(1 2)"}},
	}
	seen := map[int]bool{}
	for _, c := range canal {
		name := sanitize(c.col.RawType)
		c.col.Name = "c_" + name
		at, err := mysqlSchemaToAvroCol(c.col)
		if err != nil {
			t.Fatalf("%s: %v", c.col.RawType, err)
		}
		seen[c.col.Type] = true
		out["canal:"+name] = compatCol{col: at, snapshot: c.v[0], cdc: c.v[1]}
	}
	for typ := range mysqlschemaTypeToAvroTypeMap {
		if !seen[typ] {
			t.Fatalf("mysqlschemaTypeToAvroTypeMap has type %d: add it to compatCols and the fixtures", typ)
		}
	}
	return out
}

// sanitize makes a type name usable as an Avro field name.
func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '(' || r == ')' || r == ',' || r == '\'':
			return -1
		}
		return '_'
	}, s)
}

func compatCases(t *testing.T) []compatCase {
	cols := compatCols(t)
	var names []string
	for n := range cols {
		names = append(names, n)
	}
	sort.Strings(names)
	// One record with every mapping (the shape a wide table registers), then
	// each mapping on its own (the shape of a key schema).
	cases := []compatCase{{Name: "every mapping", Cols: names}}
	for _, n := range names {
		cases = append(cases, compatCase{Name: n, Cols: []string{n}})
	}
	return cases
}

// compatSchema builds the schema string createSchema registers, minus the
// registry call.
func compatSchema(t *testing.T, c compatCase) (string, *schemaMapper) {
	t.Helper()
	cols := compatCols(t)
	m := newSchemaMapper()
	avroCols := make([]*avroNamedType, 0, len(c.Cols))
	for _, n := range c.Cols {
		avroCols = append(avroCols, cols[n].col)
		m.colTypes[cols[n].col.Name] = cols[n].col
	}
	// The same builder createSchema registers with: name, namespace and field
	// order are pinned by the fixtures, not by a copy of them.
	rs, err := buildRecordSchema("t_payload", avroCols)
	if err != nil {
		t.Fatalf("%s: %v", c.Name, err)
	}
	return rs.String(), m
}

// compatRow runs the values through formatValue the way the iterators do.
func compatRow(t *testing.T, c compatCase, m *schemaMapper, mode string) map[string]any {
	cols := compatCols(t)
	row := map[string]any{}
	for _, n := range c.Cols {
		raw := cols[n].snapshot
		if mode == "cdc" {
			raw = cols[n].cdc
		}
		v, err := m.formatValue(context.Background(), cols[n].col.Name, raw)
		if err != nil {
			t.Fatalf("%s: %v", cols[n].col.Name, err)
		}
		// formatValue hands a BIT column over as []byte, which neither codec
		// encodes into fixed[8] (both reject it, see TestSchemaHambaCompat_BitSlice);
		// the array form is what exercises the fixed encoding.
		if b, ok := v.([]byte); ok && cols[n].col.isBit && len(b) == 8 {
			v = [8]byte(b)
		}
		row[cols[n].col.Name] = v
	}
	return row
}

var compatModes = []string{"snapshot", "cdc"}

// TestSchemaHambaCompat_BitSlice pins that a []byte for a fixed[8] field is
// rejected, as hamba rejected it.
func TestSchemaHambaCompat_BitSlice(t *testing.T) {
	cols := compatCols(t)
	c := compatCase{Name: "bit", Cols: []string{"sql:BIT"}}
	s, m := compatSchema(t, c)
	sch, err := avro.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	v, err := m.formatValue(context.Background(), cols["sql:BIT"].col.Name, cols["sql:BIT"].snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := avro.Marshal(sch, map[string]any{cols["sql:BIT"].col.Name: v}); err == nil {
		t.Fatal("expected an encode error for []byte into fixed[8]")
	}
}

func encodeCompat(sch avro.Schema, row map[string]any) (string, string) {
	enc, err := avro.Marshal(sch, row)
	if err != nil {
		return "ERR", "ERR"
	}
	decoded := map[string]any{}
	if err := avro.Unmarshal(sch, enc, &decoded); err != nil {
		return hex.EncodeToString(enc), "ERR"
	}
	return hex.EncodeToString(enc), fmt.Sprintf("%#v", decoded)
}

func TestSchemaHambaCompat(t *testing.T) {
	const path = "testdata/hamba_schema_fixtures.json"

	if os.Getenv("HAMBA_FIXTURE_GEN") == "1" {
		generateHambaFixtures(t, path)
		return
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f compatFixtureFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	cases := compatCases(t)
	if len(f.Schemas) != len(cases) {
		t.Fatalf("fixture has %d schemas, compatCases() has %d: add fixtures with HAMBA_FIXTURE_GEN=1 against hamba", len(f.Schemas), len(cases))
	}
	for i, fx := range f.Schemas {
		c := cases[i]
		t.Run(c.Name, func(t *testing.T) {
			if fx.Name != c.Name {
				t.Fatalf("fixture %q is for case %q", fx.Name, c.Name)
			}
			// The schema string registered with the Conduit schema registry
			// must not change on upgrade.
			s, m := compatSchema(t, c)
			if s != fx.Schema {
				t.Fatalf("schema text differs from what hamba/avro produced:\n old: %s\n new: %s", fx.Schema, s)
			}
			sch, err := avro.Parse(fx.Schema)
			if err != nil {
				t.Fatal(err)
			}
			for _, mode := range compatModes {
				gotHex, gotDecoded := encodeCompat(sch, compatRow(t, c, m, mode))
				if gotHex != fx.Rows[mode] {
					t.Errorf("%s: row bytes differ from hamba:\n old: %s\n new: %s", mode, fx.Rows[mode], gotHex)
				}
				// Bytes hamba wrote must decode to what hamba decoded them to.
				if fx.Rows[mode] != "ERR" {
					old, err := hex.DecodeString(fx.Rows[mode])
					if err != nil {
						t.Fatal(err)
					}
					decoded := map[string]any{}
					if err := avro.Unmarshal(sch, old, &decoded); err != nil {
						t.Fatalf("%s: decode hamba bytes: %v", mode, err)
					}
					if d := fmt.Sprintf("%#v", decoded); d != fx.Decoded[mode] {
						t.Errorf("%s: decoded values differ from hamba:\n old: %s\n new: %s", mode, fx.Decoded[mode], d)
					}
				}
				_ = gotDecoded
			}
		})
	}
}

// generateHambaFixtures writes the fixture file. Only valid when this package
// is built against github.com/hamba/avro/v2; see the file comment.
func generateHambaFixtures(t *testing.T, path string) {
	t.Helper()
	// Fixtures are the record of what hamba produced; refuse to write
	// them from any other codec.
	if pkg := reflect.TypeOf(avro.Boolean).PkgPath(); !strings.HasPrefix(pkg, "github.com/hamba/avro/") {
		t.Fatalf("HAMBA_FIXTURE_GEN=1 needs this file to import github.com/hamba/avro/v2, got %s", pkg)
	}
	var out compatFixtureFile
	for _, c := range compatCases(t) {
		s, m := compatSchema(t, c)
		sch, err := avro.Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		fx := compatFixture{compatCase: c, Schema: s, Rows: map[string]string{}, Decoded: map[string]string{}}
		for _, mode := range compatModes {
			fx.Rows[mode], fx.Decoded[mode] = encodeCompat(sch, compatRow(t, c, m, mode))
		}
		out.Schemas = append(out.Schemas, fx)
	}
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll("testdata", 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
