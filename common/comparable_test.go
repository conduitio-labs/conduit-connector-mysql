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

package common

import (
	"database/sql"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestNewComparable(t *testing.T) {
	is := is.New(t)

	tests := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"nil value", nil, true},
		{"int64", int64(42), false},
		{"int32", int32(42), false},
		{"int", 42, false},
		{"uint64", uint64(42), false},
		{"uint32", uint32(42), false},
		{"uint", uint(42), false},
		{"float64", float64(42.5), false},
		{"float32", float32(42.5), false},
		{"string", "test", false},
		{"[]byte", []byte("test"), false},
		{"time.Time", time.Now(), false},
		{"sql.NullString valid", sql.NullString{String: "test", Valid: true}, false},
		{"sql.NullString invalid", sql.NullString{String: "test", Valid: false}, true},
		{"sql.NullInt64 valid", sql.NullInt64{Int64: 42, Valid: true}, false},
		{"sql.NullInt64 invalid", sql.NullInt64{Int64: 42, Valid: false}, true},
		{"sql.NullFloat64 valid", sql.NullFloat64{Float64: 42.5, Valid: true}, false},
		{"sql.NullFloat64 invalid", sql.NullFloat64{Float64: 42.5, Valid: false}, true},
		{"sql.NullTime valid", sql.NullTime{Time: time.Now(), Valid: true}, false},
		{"sql.NullTime invalid", sql.NullTime{Time: time.Now(), Valid: false}, true},
		{"unsupported type", struct{}{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			got, err := NewComparable(tt.input)
			is.Equal((err != nil), tt.wantErr)
			if !tt.wantErr {
				is.True(got != nil)
			}
		})
	}
}

func TestIntComparable(t *testing.T) {
	is := is.New(t)

	tests := []struct {
		name       string
		value      int64
		otherValue int64
		wantLess   bool
		wantEqual  bool
	}{
		{"less than", 1, 2, true, false},
		{"greater than", 2, 1, false, false},
		{"equal", 1, 1, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			c := &IntComparable{Value: tt.value}
			other := &IntComparable{Value: tt.otherValue}

			is.Equal(c.Less(other), tt.wantLess)
			is.Equal(c.Equal(other), tt.wantEqual)

			// Test nil comparison
			is.True(!c.Less(nil))
			is.True(!c.Equal(nil))

			// Test String() method
			is.True(c.String() != "")
		})
	}
}

func TestUintComparable(t *testing.T) {
	is := is.New(t)

	tests := []struct {
		name       string
		value      uint64
		otherValue uint64
		wantLess   bool
		wantEqual  bool
	}{
		{"less than", 1, 2, true, false},
		{"greater than", 2, 1, false, false},
		{"equal", 1, 1, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			c := &UintComparable{Value: tt.value}
			other := &UintComparable{Value: tt.otherValue}

			is.Equal(c.Less(other), tt.wantLess)
			is.Equal(c.Equal(other), tt.wantEqual)

			// Test nil comparison
			is.True(!c.Less(nil))
			is.True(!c.Equal(nil))

			// Test String() method
			is.True(c.String() != "")
		})
	}
}

func TestStringComparable(t *testing.T) {
	is := is.New(t)

	tests := []struct {
		name       string
		value      string
		otherValue string
		wantLess   bool
		wantEqual  bool
	}{
		{"less than", "a", "b", true, false},
		{"greater than", "b", "a", false, false},
		{"equal", "a", "a", false, true},
		{"empty strings", "", "", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			c := &StringComparable{Value: tt.value}
			other := &StringComparable{Value: tt.otherValue}

			is.Equal(c.Less(other), tt.wantLess)
			is.Equal(c.Equal(other), tt.wantEqual)

			// Test nil comparison
			is.True(!c.Less(nil))
			is.True(!c.Equal(nil))

			// Test String() method
			is.Equal(c.String(), tt.value)
		})
	}
}

func TestTimeComparable(t *testing.T) {
	is := is.New(t)

	now := time.Now()
	later := now.Add(time.Hour)

	tests := []struct {
		name       string
		value      time.Time
		otherValue time.Time
		wantLess   bool
		wantEqual  bool
	}{
		{"less than", now, later, true, false},
		{"greater than", later, now, false, false},
		{"equal", now, now, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			c := &TimeComparable{Value: tt.value}
			other := &TimeComparable{Value: tt.otherValue}

			is.Equal(c.Less(other), tt.wantLess)
			is.Equal(c.Equal(other), tt.wantEqual)

			// Test nil comparison
			is.True(!c.Less(nil))
			is.True(!c.Equal(nil))

			// Test String() method
			is.True(c.String() != "")
		})
	}
}

func TestFloatComparable(t *testing.T) {
	is := is.New(t)

	tests := []struct {
		name       string
		value      float64
		otherValue float64
		wantLess   bool
		wantEqual  bool
	}{
		{"less than", 1.0, 2.0, true, false},
		{"greater than", 2.0, 1.0, false, false},
		{"equal", 1.0, 1.0, false, true},
		{"zero values", 0.0, 0.0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			c := &FloatComparable{Value: tt.value}
			other := &FloatComparable{Value: tt.otherValue}

			is.Equal(c.Less(other), tt.wantLess)
			is.Equal(c.Equal(other), tt.wantEqual)

			// Test nil comparison
			is.True(!c.Less(nil))
			is.True(!c.Equal(nil))

			// Test String() method
			is.True(c.String() != "")
		})
	}
}

func TestTypeMismatch(t *testing.T) {
	tests := []struct {
		name string
		a    Comparable
		b    Comparable
	}{
		{"int vs string", &IntComparable{1}, &StringComparable{"1"}},
		{"float vs int", &FloatComparable{1.0}, &IntComparable{1}},
		{"string vs time", &StringComparable{"2024-01-01"}, &TimeComparable{time.Now()}},
		{"uint vs int", &UintComparable{1}, &IntComparable{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(_ *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("Expected panic for type mismatch")
				}
			}()
			tt.a.Less(tt.b)
		})

		t.Run(tt.name+"_equal", func(_ *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("Expected panic for type mismatch")
				}
			}()
			tt.a.Equal(tt.b)
		})
	}
}
