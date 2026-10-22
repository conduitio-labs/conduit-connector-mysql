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

package debezium

import (
	"math"
	"strings"
	"testing"
)

// Errors reach the DLQ, the API and CLI output, so a row value is never in
// them.
func TestFormatErrors_DoNotContainTheValue(t *testing.T) {
	f := Field{Name: "c"}
	cases := map[string]func() error{
		"year":        func() error { _, err := formatYear(f, "SECRET-year"); return err },
		"float":       func() error { _, err := formatFloat(f, "SECRET-float", 64); return err },
		"int range":   func() error { _, err := formatInt(f, int64(70000), -32768, 32767); return err },
		"uint64 over": func() error { _, err := formatInt64(f, uint64(math.MaxUint64)); return err },
	}
	for name, call := range cases {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil {
				t.Fatal("want an error")
			}
			for _, secret := range []string{"SECRET", "70000", "18446744073709551615"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error repeats the value: %v", err)
				}
			}
		})
	}
}
