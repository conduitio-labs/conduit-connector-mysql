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
	"math"
	"testing"
	"time"
)

func TestFormatValue_NullStaysNull(t *testing.T) {
	t.Parallel()

	got, err := FormatValue(Field{Name: "x", Type: TypeInt64}, nil)
	if err != nil {
		t.Fatalf("FormatValue(nil) error: %v", err)
	}
	if got != nil {
		t.Fatalf("FormatValue(nil) = %v, want nil", got)
	}
}

func TestFormatValue_BigIntUnsignedOverflowHalts(t *testing.T) {
	t.Parallel()

	f := Field{Name: "id", Type: TypeInt64}
	_, err := FormatValue(f, uint64(math.MaxInt64)+1)
	var overflow UnsignedOverflowError
	if !errors.As(err, &overflow) {
		t.Fatalf("expected UnsignedOverflowError, got %v", err)
	}
	if overflow.Column != "id" {
		t.Errorf("overflow.Column = %q, want %q", overflow.Column, "id")
	}

	// The largest in-range value passes.
	got, err := FormatValue(f, uint64(math.MaxInt64))
	if err != nil {
		t.Fatalf("FormatValue(max int64) error: %v", err)
	}
	if got != int64(math.MaxInt64) {
		t.Errorf("FormatValue(max int64) = %v (%T), want int64 max", got, got)
	}
}

func TestFormatValue_Decimal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts Options
		want any
	}{
		{"precise", DefaultOptions(), "1.10"},
		{"string", Options{DecimalHandlingMode: DecimalString}, "1.10"},
		{"double", Options{DecimalHandlingMode: DecimalDouble}, 1.1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			col := Column{Name: "price", BaseType: "decimal", Precision: 10, Scale: 2}
			f, err := MapColumn(col, tt.opts)
			if err != nil {
				t.Fatalf("MapColumn: %v", err)
			}
			got, err := FormatValue(f, []byte("1.10"))
			if err != nil {
				t.Fatalf("FormatValue: %v", err)
			}
			if got != tt.want {
				t.Errorf("FormatValue = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestFormatValue_Temporal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		col  Column
		in   any
		want any
	}{
		{"date string", Column{Name: "d", BaseType: "date"}, "1970-01-02", int32(1)},
		{"date time", Column{Name: "d", BaseType: "date"}, time.Date(1970, 1, 2, 0, 0, 0, 0, time.UTC), int32(1)},
		{"time string", Column{Name: "t", BaseType: "time"}, "00:00:01", int64(1_000_000)},
		{"time fractional", Column{Name: "t", BaseType: "time"}, "00:00:01.5", int64(1_500_000)},
		{"datetime p0", Column{Name: "ts", BaseType: "datetime"}, "1970-01-01 00:00:01", int64(1000)},
		{"datetime p6", Column{Name: "ts", BaseType: "datetime", Precision: 6}, "1970-01-01 00:00:01", int64(1_000_000)},
		{"timestamp", Column{Name: "ts", BaseType: "timestamp"}, "1970-01-01 00:00:01", "1970-01-01T00:00:01.000000Z"},
		{"year", Column{Name: "y", BaseType: "year"}, []byte("2024"), int32(2024)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, err := MapColumn(tt.col, DefaultOptions())
			if err != nil {
				t.Fatalf("MapColumn: %v", err)
			}
			got, err := FormatValue(f, tt.in)
			if err != nil {
				t.Fatalf("FormatValue: %v", err)
			}
			if got != tt.want {
				t.Errorf("FormatValue = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestFormatValue_Bits(t *testing.T) {
	t.Parallel()

	one, _ := MapColumn(Column{Name: "flag", BaseType: "bit", Precision: 1}, DefaultOptions())
	got, err := FormatValue(one, []byte{1})
	if err != nil {
		t.Fatalf("BIT(1) FormatValue: %v", err)
	}
	if got != true {
		t.Errorf("BIT(1) = %v, want true", got)
	}

	eight, _ := MapColumn(Column{Name: "mask", BaseType: "bit", Precision: 8}, DefaultOptions())
	// The driver returns big-endian bytes; Debezium stores little-endian.
	got, err = FormatValue(eight, []byte{0b1000_0001})
	if err != nil {
		t.Fatalf("BIT(8) FormatValue: %v", err)
	}
	if b := got.([]byte); len(b) != 1 || b[0] != 0b1000_0001 {
		t.Errorf("BIT(8) = %v, want [0b10000001]", got)
	}

	// canal returns BIT as an int64.
	got, err = FormatValue(eight, int64(0x01))
	if err != nil {
		t.Fatalf("BIT(8) int64 FormatValue: %v", err)
	}
	if b := got.([]byte); len(b) != 1 || b[0] != 0x01 {
		t.Errorf("BIT(8) int64 = %v, want [0x01]", got)
	}
}

func TestFormatValue_Integers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		col  Column
		in   any
		want any
	}{
		{"tinyint", Column{Name: "x", BaseType: "tinyint"}, int8(-5), int16(-5)},
		{"int", Column{Name: "x", BaseType: "int"}, int32(42), int32(42)},
		{"bigint", Column{Name: "x", BaseType: "bigint"}, int64(42), int64(42)},
		{"int unsigned", Column{Name: "x", BaseType: "int", Unsigned: true}, uint32(42), int64(42)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, err := MapColumn(tt.col, DefaultOptions())
			if err != nil {
				t.Fatalf("MapColumn: %v", err)
			}
			got, err := FormatValue(f, tt.in)
			if err != nil {
				t.Fatalf("FormatValue: %v", err)
			}
			if got != tt.want {
				t.Errorf("FormatValue = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}
