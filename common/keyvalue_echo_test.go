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

package common

import (
	"strings"
	"testing"
)

// A snapshot key is record data: the errors for a saved key that cannot be
// decoded must not repeat it.
func TestDecodeSnapshotKey_ErrorsDoNotContainTheValue(t *testing.T) {
	cases := map[string]func() error{
		"legacy number": func() error { _, err := decodeLegacyNumber("SECRET-key-4711"); return err },
		"int64": func() error {
			_, err := decodeTagged(taggedKeyValue{Type: keyTypeInt64, Value: "SECRET-key-4711"})
			return err
		},
		"uint64": func() error {
			_, err := decodeTagged(taggedKeyValue{Type: keyTypeUint64, Value: "SECRET-key-4711"})
			return err
		},
		"float32": func() error {
			_, err := decodeTagged(taggedKeyValue{Type: keyTypeFloat32, Value: "SECRET-key-4711"})
			return err
		},
		"float64": func() error {
			_, err := decodeTagged(taggedKeyValue{Type: keyTypeFloat64, Value: "SECRET-key-4711"})
			return err
		},
		"time": func() error {
			_, err := decodeTagged(taggedKeyValue{Type: keyTypeTime, Value: "SECRET-4711"})
			return err
		},
		"bool": func() error {
			_, err := decodeTagged(taggedKeyValue{Type: keyTypeBool, Value: "SECRET-4711"})
			return err
		},
		"bytes": func() error {
			_, err := decodeTagged(taggedKeyValue{Type: keyTypeBytes, Value: "SECRET-4711!"})
			return err
		},
		"float64 NaN": func() error { _, err := decodeTagged(taggedKeyValue{Type: keyTypeFloat64, Value: "NaN"}); return err },
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "4711") {
				t.Errorf("error repeats the key: %v", err)
			}
		})
	}
}
