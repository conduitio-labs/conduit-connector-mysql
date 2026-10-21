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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// logHashSalt is random per process, so a logged hash cannot be reversed by
// hashing guessed values (primary keys are often sequential ids) and cannot be
// matched across runs or between processes.
var logHashSalt = func() []byte {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("cannot read random bytes for the log hash: %v", err))
	}
	return b
}()

// logHash returns a short, salted digest of a primary key value (or key
// tuple) for log lines. Two lines about the same value in one process get the
// same digest, so they can be correlated without writing the value to the
// log. Never log a key, a row or a query argument; log its size and this.
func logHash(v any) string {
	mac := hmac.New(sha256.New, logHashSalt)
	fmt.Fprintf(mac, "%T:%#v", v, v)
	return hex.EncodeToString(mac.Sum(nil)[:8])
}
