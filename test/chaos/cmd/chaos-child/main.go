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

//go:build chaos

// Command chaos-child runs the SIGKILL harness's child process (chaos.RunChild)
// as a plain program. The nightly upgrade and downgrade workflow builds it from
// the last release of the connector, with this harness copied in, so that test
// can run the released source and this one against the same positions.
package main

import (
	"os"

	"github.com/conduitio-labs/conduit-connector-mysql/test/chaos"
)

func main() {
	os.Exit(chaos.RunChild())
}
