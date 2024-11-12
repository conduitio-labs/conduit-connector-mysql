package common

import (
	"testing"

	"github.com/matryer/is"
)

func TestAreEqual(t *testing.T) {
	tests := []struct {
		name        string
		a           any
		b           any
		wantEqual   bool
		wantCantCmp bool
	}{
		{name: "identical integers", a: 42, b: 42, wantEqual: true, wantCantCmp: false},
		{name: "different integers", a: 42, b: 43, wantEqual: false, wantCantCmp: false},
		{name: "identical strings", a: "hello", b: "hello", wantEqual: true, wantCantCmp: false},
		{name: "different strings", a: "hello", b: "world", wantEqual: false, wantCantCmp: false},

		{name: "string and []uint8", a: "hello", b: []uint8("hello"), wantEqual: true, wantCantCmp: false},
		{name: "[]byte and string", a: []byte("hello"), b: "hello", wantEqual: true, wantCantCmp: false},

		{name: "identical []uint8", a: []uint8("hello"), b: []uint8("hello"), wantEqual: true, wantCantCmp: false},
		{name: "different []uint8", a: []uint8("hello"), b: []uint8("world"), wantEqual: false, wantCantCmp: false},
		{name: "identical []byte", a: []byte("hello"), b: []byte("hello"), wantEqual: true, wantCantCmp: false},
		{name: "different []byte", a: []byte("hello"), b: []byte("world"), wantEqual: false, wantCantCmp: false},
		{name: "mixed []byte and []uint8", a: []byte("hello"), b: []uint8("hello"), wantEqual: true, wantCantCmp: false},
		{name: "nil []byte", a: []byte(nil), b: []byte(nil), wantEqual: true, wantCantCmp: false},
		{name: "empty []byte", a: []byte{}, b: []byte{}, wantEqual: true, wantCantCmp: false},
		{name: "one nil", a: []byte("hello"), b: nil, wantEqual: false, wantCantCmp: false},
		{name: "both nil interface", a: nil, b: nil, wantEqual: true, wantCantCmp: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			gotEqual, gotCantCmp := AreEqual(tt.a, tt.b)
			is.Equal(gotEqual, tt.wantEqual)     // gotEqual != wantEqual
			is.Equal(gotCantCmp, tt.wantCantCmp) // gotCantCmp != wantCantCmp
		})
	}
}

// TestAreEqualSymmetric ensures that AreEqual is symmetric
// (i.e., AreEqual(a, b) == AreEqual(b, a))
func TestAreEqualSymmetric(t *testing.T) {
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
