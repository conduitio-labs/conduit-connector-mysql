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
	"math"
	"testing"

	"github.com/matryer/is"
)

func TestIsGreaterOrEqual(t *testing.T) {
	tests := []struct {
		name        string
		a           any
		b           any
		wantGreater bool
		wantCantCmp bool
	}{
		// Identical values
		{name: "identical integers", a: 42, b: 42, wantGreater: true, wantCantCmp: false},
		{name: "identical strings", a: "hello", b: "hello", wantGreater: true, wantCantCmp: false},
		{name: "identical []uint8", a: []uint8("hello"), b: []uint8("hello"), wantGreater: true, wantCantCmp: false},
		{name: "identical []byte", a: []byte("hello"), b: []byte("hello"), wantGreater: true, wantCantCmp: false},
		{name: "nil []byte", a: []byte(nil), b: []byte(nil), wantGreater: true, wantCantCmp: false},
		{name: "empty []byte", a: []byte{}, b: []byte{}, wantGreater: true, wantCantCmp: false},

		// Greater and less comparisons
		{name: "a greater than b integers", a: 43, b: 42, wantGreater: true, wantCantCmp: false},
		{name: "b greater than a integers", a: 42, b: 43, wantGreater: false, wantCantCmp: false},
		{name: "a greater than b strings", a: "world", b: "hello", wantGreater: true, wantCantCmp: false},
		{name: "b greater than a strings", a: "hello", b: "world", wantGreater: false, wantCantCmp: false},
		{name: "a greater than b []uint8", a: []uint8("world"), b: []uint8("hello"), wantGreater: true, wantCantCmp: false},
		{name: "b greater than a []uint8", a: []uint8("hello"), b: []uint8("world"), wantGreater: false, wantCantCmp: false},
		{name: "a greater than b []byte", a: []byte("world"), b: []byte("hello"), wantGreater: true, wantCantCmp: false},
		{name: "b greater than a []byte", a: []byte("hello"), b: []byte("world"), wantGreater: false, wantCantCmp: false},

		// Mixed types
		{name: "string and []uint8", a: "hello", b: []uint8("hello"), wantGreater: true, wantCantCmp: false},
		{name: "[]byte and string", a: []byte("hello"), b: "hello", wantGreater: true, wantCantCmp: false},
		{name: "mixed []byte and []uint8", a: []byte("hello"), b: []uint8("hello"), wantGreater: true, wantCantCmp: false},
		{name: "one nil", a: []byte("hello"), b: nil, wantGreater: false, wantCantCmp: true},

		// Integer types - same type
		{name: "int64 equal", a: int64(42), b: int64(42), wantGreater: true, wantCantCmp: false},
		{name: "int64 greater", a: int64(43), b: int64(42), wantGreater: true, wantCantCmp: false},
		{name: "int64 less", a: int64(41), b: int64(42), wantGreater: false, wantCantCmp: false},

		{name: "uint equal", a: uint(42), b: uint(42), wantGreater: true, wantCantCmp: false},
		{name: "uint greater", a: uint(43), b: uint(42), wantGreater: true, wantCantCmp: false},
		{name: "uint less", a: uint(41), b: uint(42), wantGreater: false, wantCantCmp: false},

		// Float types
		{name: "float64 equal", a: float64(42.0), b: float64(42.0), wantGreater: true, wantCantCmp: false},
		{name: "float64 greater", a: float64(42.1), b: float64(42.0), wantGreater: true, wantCantCmp: false},
		{name: "float64 less", a: float64(41.9), b: float64(42.0), wantGreater: false, wantCantCmp: false},

		{name: "float32 equal", a: float32(42.0), b: float32(42.0), wantGreater: true, wantCantCmp: false},
		{name: "float32 greater", a: float32(42.1), b: float32(42.0), wantGreater: true, wantCantCmp: false},
		{name: "float32 less", a: float32(41.9), b: float32(42.0), wantGreater: false, wantCantCmp: false},

		// Cross-type numeric comparisons
		{name: "int vs float64 equal", a: 42, b: float64(42.0), wantGreater: true, wantCantCmp: false},
		{name: "int vs float64 greater", a: 43, b: float64(42.5), wantGreater: true, wantCantCmp: false},
		{name: "int vs float64 less", a: 42, b: float64(42.1), wantGreater: false, wantCantCmp: false},

		{name: "uint vs int equal", a: uint(42), b: 42, wantGreater: true, wantCantCmp: false},
		{name: "uint vs int greater", a: uint(43), b: 42, wantGreater: true, wantCantCmp: false},
		{name: "uint vs int less", a: uint(41), b: 42, wantGreater: false, wantCantCmp: false},

		{name: "int64 vs float32 equal", a: int64(42), b: float32(42.0), wantGreater: true, wantCantCmp: false},
		{name: "int64 vs float32 greater", a: int64(43), b: float32(42.5), wantGreater: true, wantCantCmp: false},
		{name: "int64 vs float32 less", a: int64(42), b: float32(42.1), wantGreater: false, wantCantCmp: false},

		// Edge cases
		{name: "uint vs negative int", a: uint(1), b: -1, wantGreater: true, wantCantCmp: false},
		{name: "negative int vs uint", a: -1, b: uint(1), wantGreater: false, wantCantCmp: false},
		{name: "max uint64 vs float64", a: uint64(math.MaxUint64), b: float64(math.MaxUint64), wantGreater: true, wantCantCmp: false},
		{name: "max int64 vs float64", a: int64(math.MaxInt64), b: float64(math.MaxInt64), wantGreater: true, wantCantCmp: false},

		// Invalid comparisons
		{name: "number vs string", a: 42, b: "42", wantGreater: false, wantCantCmp: true},
		{name: "float64 vs []byte", a: float64(42), b: []byte("42"), wantGreater: false, wantCantCmp: true},
		{name: "int vs nil", a: 42, b: nil, wantGreater: false, wantCantCmp: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			gotGreater, gotCantCmp := IsGreaterOrEqual(tt.a, tt.b)
			is.Equal(gotGreater, tt.wantGreater) // gotGreater != wantGreater
			is.Equal(gotCantCmp, tt.wantCantCmp) // gotCantCmp != wantCantCmp
		})
	}
}

// TestIsGreaterOrEqualSymmetric ensures that IsGreaterOrEqual is symmetric
// (i.e., IsGreaterOrEqual(a, b) == !IsGreaterOrEqual(b, a)).
func TestIsGreaterOrEqualSymmetric(t *testing.T) {
	is := is.New(t)
	testCases := []struct {
		a, b any
	}{
		{[]byte("1hello"), []uint8("2hello")},
		{42, 43},
		{-1, uint(1)},                           // edge case with negative numbers
		{int64(math.MaxInt64), math.MaxFloat64}, // edge case with large numbers
	}

	for _, tc := range testCases {
		greater1, cantCmp1 := IsGreaterOrEqual(tc.a, tc.b)
		greater2, cantCmp2 := IsGreaterOrEqual(tc.b, tc.a)

		is.Equal(greater1, !greater2) // greater result should be symmetric
		is.Equal(cantCmp1, cantCmp2)  // cantCompare result should be symmetric
	}
}
