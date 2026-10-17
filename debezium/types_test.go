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

package debezium

import (
	"errors"
	"testing"
)

func TestNormalizeType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw      string
		base     string
		unsigned bool
	}{
		{"UNSIGNED BIGINT", "bigint", true},
		{"bigint unsigned", "bigint", true},
		{"BIGINT", "bigint", false},
		{"INT", "int", false},
		{"decimal(10,2)", "decimal", false},
		{"varchar(255)", "varchar", false},
		{"bit(8)", "bit", false},
		{"enum('a','b')", "enum", false},
		{"TIMESTAMP", "timestamp", false},
		{"  unsigned int ", "int", true},
	}
	for _, tt := range tests {
		base, unsigned := NormalizeType(tt.raw)
		if base != tt.base || unsigned != tt.unsigned {
			t.Errorf("NormalizeType(%q) = (%q, %v), want (%q, %v)", tt.raw, base, unsigned, tt.base, tt.unsigned)
		}
	}
}

func TestMapColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		col      Column
		opts     Options
		want     Field
		wantType Type
		semantic string
		params   map[string]string
	}{
		{
			name:     "tinyint",
			col:      Column{Name: "id", BaseType: "tinyint"},
			wantType: TypeInt16,
		},
		{
			name:     "smallint unsigned",
			col:      Column{Name: "id", BaseType: "smallint", Unsigned: true},
			wantType: TypeInt32,
		},
		{
			name:     "int",
			col:      Column{Name: "id", BaseType: "int"},
			wantType: TypeInt32,
		},
		{
			name:     "int unsigned",
			col:      Column{Name: "id", BaseType: "int", Unsigned: true},
			wantType: TypeInt64,
		},
		{
			name:     "bigint",
			col:      Column{Name: "id", BaseType: "bigint"},
			wantType: TypeInt64,
		},
		{
			name:     "bigint unsigned long",
			col:      Column{Name: "id", BaseType: "bigint", Unsigned: true},
			wantType: TypeInt64,
		},
		{
			name:     "bigint unsigned precise",
			col:      Column{Name: "id", BaseType: "bigint", Unsigned: true},
			opts:     Options{BigIntUnsignedHandlingMode: BigIntUnsignedPrecise},
			wantType: TypeBytes,
			semantic: SemanticDecimal,
			params:   map[string]string{"scale": "0", "connect.decimal.precision": "20"},
		},
		{
			name:     "bigint unsigned string",
			col:      Column{Name: "id", BaseType: "bigint", Unsigned: true},
			opts:     Options{BigIntUnsignedHandlingMode: BigIntUnsignedString},
			wantType: TypeString,
		},
		{
			name:     "decimal precise",
			col:      Column{Name: "price", BaseType: "decimal", Precision: 10, Scale: 2},
			wantType: TypeBytes,
			semantic: SemanticDecimal,
			params:   map[string]string{"scale": "2", "connect.decimal.precision": "10"},
		},
		{
			name:     "decimal double",
			col:      Column{Name: "price", BaseType: "decimal", Precision: 10, Scale: 2},
			opts:     Options{DecimalHandlingMode: DecimalDouble},
			wantType: TypeFloat64,
		},
		{
			name:     "decimal string",
			col:      Column{Name: "price", BaseType: "decimal", Precision: 10, Scale: 2},
			opts:     Options{DecimalHandlingMode: DecimalString},
			wantType: TypeString,
		},
		{
			name:     "bit1",
			col:      Column{Name: "flag", BaseType: "bit", Precision: 1},
			wantType: TypeBoolean,
		},
		{
			name:     "bit8",
			col:      Column{Name: "mask", BaseType: "bit", Precision: 8},
			wantType: TypeBytes,
			semantic: SemanticBits,
			params:   map[string]string{"length": "8"},
		},
		{
			name:     "date",
			col:      Column{Name: "d", BaseType: "date"},
			wantType: TypeInt32,
			semantic: SemanticDate,
		},
		{
			name:     "date connect",
			col:      Column{Name: "d", BaseType: "date"},
			opts:     Options{TimePrecisionMode: TimePrecisionConnect},
			wantType: TypeInt32,
			semantic: SemanticConnectDate,
		},
		{
			name:     "datetime p0",
			col:      Column{Name: "ts", BaseType: "datetime"},
			wantType: TypeInt64,
			semantic: SemanticTimestamp,
		},
		{
			name:     "datetime p6",
			col:      Column{Name: "ts", BaseType: "datetime", Precision: 6},
			wantType: TypeInt64,
			semantic: SemanticMicroTimestamp,
		},
		{
			name:     "timestamp",
			col:      Column{Name: "ts", BaseType: "timestamp"},
			wantType: TypeString,
			semantic: SemanticZonedTimestamp,
		},
		{
			name:     "enum",
			col:      Column{Name: "color", BaseType: "enum"},
			wantType: TypeString,
			semantic: SemanticEnum,
		},
		{
			name:     "set",
			col:      Column{Name: "tags", BaseType: "set"},
			wantType: TypeString,
			semantic: SemanticEnumSet,
		},
		{
			name:     "json",
			col:      Column{Name: "doc", BaseType: "json"},
			wantType: TypeString,
			semantic: SemanticJSON,
		},
		{
			name:     "varchar",
			col:      Column{Name: "name", BaseType: "varchar"},
			wantType: TypeString,
		},
		{
			name:     "blob",
			col:      Column{Name: "data", BaseType: "blob"},
			wantType: TypeBytes,
		},
		{
			name:     "blob base64",
			col:      Column{Name: "data", BaseType: "blob"},
			opts:     Options{BinaryHandlingMode: BinaryBase64},
			wantType: TypeString,
		},
		{
			name:     "year",
			col:      Column{Name: "y", BaseType: "year"},
			wantType: TypeInt32,
			semantic: SemanticYear,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := tt.opts
			if opts == (Options{}) {
				opts = DefaultOptions()
			}
			got, err := MapColumn(tt.col, opts)
			if err != nil {
				t.Fatalf("MapColumn(%+v) error: %v", tt.col, err)
			}
			if got.Type != tt.wantType {
				t.Errorf("Type = %q, want %q", got.Type, tt.wantType)
			}
			if got.Semantic != tt.semantic {
				t.Errorf("Semantic = %q, want %q", got.Semantic, tt.semantic)
			}
			if len(got.Parameters) != len(tt.params) {
				t.Fatalf("Parameters = %v, want %v", got.Parameters, tt.params)
			}
			for k, v := range tt.params {
				if got.Parameters[k] != v {
					t.Errorf("Parameters[%q] = %q, want %q", k, got.Parameters[k], v)
				}
			}
			if got.Optional != tt.col.Nullable {
				t.Errorf("Optional = %v, want %v", got.Optional, tt.col.Nullable)
			}
		})
	}
}

func TestMapColumn_UnknownType(t *testing.T) {
	t.Parallel()

	_, err := MapColumn(Column{Name: "x", BaseType: "madeup"}, DefaultOptions())
	if err == nil {
		t.Fatal("expected an error for an unknown type")
	}
	var unknown UnknownTypeError
	if !errors.As(err, &unknown) {
		t.Fatalf("expected UnknownTypeError, got %T", err)
	}
}
