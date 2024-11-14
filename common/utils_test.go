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
		{name: "identical integers", a: 42, b: 42, wantGreater: true, wantCantCmp: false},
		{name: "a greater than b integers", a: 43, b: 42, wantGreater: true, wantCantCmp: false},
		{name: "b greater than a integers", a: 42, b: 43, wantGreater: false, wantCantCmp: false},
		{name: "identical strings", a: "hello", b: "hello", wantGreater: true, wantCantCmp: false},
		{name: "a greater than b strings", a: "world", b: "hello", wantGreater: true, wantCantCmp: false},
		{name: "b greater than a strings", a: "hello", b: "world", wantGreater: false, wantCantCmp: false},

		{name: "string and []uint8", a: "hello", b: []uint8("hello"), wantGreater: true, wantCantCmp: false},
		{name: "[]byte and string", a: []byte("hello"), b: "hello", wantGreater: true, wantCantCmp: false},

		{name: "identical []uint8", a: []uint8("hello"), b: []uint8("hello"), wantGreater: true, wantCantCmp: false},
		{name: "a greater than b []uint8", a: []uint8("world"), b: []uint8("hello"), wantGreater: true, wantCantCmp: false},
		{name: "b greater than a []uint8", a: []uint8("hello"), b: []uint8("world"), wantGreater: false, wantCantCmp: false},
		{name: "identical []byte", a: []byte("hello"), b: []byte("hello"), wantGreater: true, wantCantCmp: false},
		{name: "a greater than b []byte", a: []byte("world"), b: []byte("hello"), wantGreater: true, wantCantCmp: false},
		{name: "b greater than a []byte", a: []byte("hello"), b: []byte("world"), wantGreater: false, wantCantCmp: false},
		{name: "mixed []byte and []uint8", a: []byte("hello"), b: []uint8("hello"), wantGreater: true, wantCantCmp: false},
		{name: "nil []byte", a: []byte(nil), b: []byte(nil), wantGreater: true, wantCantCmp: false},
		{name: "empty []byte", a: []byte{}, b: []byte{}, wantGreater: true, wantCantCmp: false},
		{name: "one nil", a: []byte("hello"), b: nil, wantGreater: false, wantCantCmp: true},
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
		{[]byte("hello"), []uint8("hello")},
		{42, 42},
		{"hello", "hello"},
		{[]byte(nil), []byte(nil)},
	}

	for _, tc := range testCases {
		equal1, cantCmp1 := AreEqual(tc.a, tc.b)
		equal2, cantCmp2 := AreEqual(tc.b, tc.a)

		is.Equal(equal1, equal2)     // equality result should be symmetric
		is.Equal(cantCmp1, cantCmp2) // cantCompare result should be symmetric
	}
}
