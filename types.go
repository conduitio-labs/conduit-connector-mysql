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
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
)

// Type modes and values. See ConduitIO/conduit
// docs/design-documents/20261012-mysql-type-semantics.md for the design and
// the transition table resolveTypes implements.
const (
	typesModeStrict = "strict"
	typesModeLegacy = "legacy"

	typesDecimalString = "string"
	typesDecimalDouble = "double"

	typesUnsignedLong   = "long"
	typesUnsignedString = "string"

	// typesValueDecimal is designed (the Avro decimal logical type) but not in
	// this version: it is refused with CodeTypesInvalid.
	typesValueDecimal = "decimal"

	legacyStampName = "legacy"
)

// typeConfig is a resolved type configuration.
type typeConfig struct {
	mode           string
	decimal        string
	bigintUnsigned string
}

func (c typeConfig) strict() bool { return c.mode == typesModeStrict }

// stamp is what positions written under c carry; nil for legacy.
func (c typeConfig) stamp() *common.TypesStamp {
	if !c.strict() {
		return nil
	}
	return &common.TypesStamp{Mode: c.mode, Decimal: c.decimal, BigintUnsigned: c.bigintUnsigned}
}

func (c typeConfig) name() string { return c.stamp().Name() }

var legacyTypes = typeConfig{mode: typesModeLegacy, decimal: typesDecimalDouble, bigintUnsigned: typesUnsignedLong}

func typesFromStamp(s *common.TypesStamp) typeConfig {
	if s == nil {
		return legacyTypes
	}
	return typeConfig{mode: s.Mode, decimal: s.Decimal, bigintUnsigned: s.BigintUnsigned}
}

// typePhase is where a pipeline is, as the next Open would read it: no
// position, a snapshot position, or a CDC position.
type typePhase int32

const (
	phaseNone typePhase = iota
	phaseSnapshot
	phaseCDC
)

var phaseNames = [...]string{
	phaseNone: "none", phaseSnapshot: string(common.PositionTypeSnapshot), phaseCDC: string(common.PositionTypeCDC),
}

func (p typePhase) String() string {
	if int(p) < len(phaseNames) {
		return phaseNames[p]
	}
	return "unknown"
}

// explicitTypes are the types.* settings as the operator wrote them; "" is
// unset.
type explicitTypes struct {
	mode, decimal, bigintUnsigned, acceptChangeFrom string
}

func (e explicitTypes) validate() error {
	invalid := func(path, value, allowed string) error {
		return newError(ErrTypesInvalid, CodeTypesInvalid, path,
			"set "+path+" to one of "+allowed+", or remove it",
			"%s=%q is not supported by this version", path, value)
	}
	switch e.mode {
	case "", typesModeStrict, typesModeLegacy:
	default:
		return invalid("types.mode", e.mode, "strict, legacy")
	}
	switch e.decimal {
	case "", typesDecimalString, typesDecimalDouble:
	case typesValueDecimal:
		return newError(ErrTypesInvalid, CodeTypesInvalid, "types.decimal",
			"use types.decimal=string (exact text) or double",
			"types.decimal=decimal (the Avro decimal logical type) is not available in this version")
	default:
		return invalid("types.decimal", e.decimal, "string, double")
	}
	switch e.bigintUnsigned {
	case "", typesUnsignedLong, typesUnsignedString:
	case typesValueDecimal:
		return newError(ErrTypesInvalid, CodeTypesInvalid, "types.bigintUnsigned",
			"use types.bigintUnsigned=string for the full range, or long",
			"types.bigintUnsigned=decimal (the Avro decimal logical type) is not available in this version")
	default:
		return invalid("types.bigintUnsigned", e.bigintUnsigned, "long, string")
	}
	return nil
}

// typeResolution is the outcome of resolveTypes.
type typeResolution struct {
	config typeConfig
	// switchedFrom names the stamp being replaced when the configuration
	// changes at this Open (transition table rows 6 and 11); "" otherwise.
	switchedFrom string
	// warnings are logged at Open.
	warnings []string
}

// resolveTypes is the transition table: it derives the type configuration
// for this Open from the explicit settings and the position, or refuses.
// positioned says whether there is a position at all; stamp is its type
// stamp (nil: legacy). Unset settings are taken from the stamp, so a default
// that changes between versions never causes a mismatch by itself.
func resolveTypes(explicit explicitTypes, positioned bool, stamp *common.TypesStamp, phase typePhase) (typeResolution, error) {
	if err := explicit.validate(); err != nil {
		return typeResolution{}, err
	}

	// Row 1 and 2: no position.
	if !positioned {
		cfg, err := completeTypes(explicit, nil)
		if err != nil {
			return typeResolution{}, err
		}
		res := typeResolution{config: cfg}
		if explicit.mode == "" {
			res.warnings = append(res.warnings, CodeLegacyTypes+": types.mode is not set and the pipeline has no position, "+
				"so it runs in legacy mode (NULL delivered as the zero value, DECIMAL as double). Set types.mode=strict "+
				"for new pipelines; a later version will require types.mode to be set")
		}
		return res, nil
	}

	current := typesFromStamp(stamp)
	cfg, err := completeTypes(explicit, stamp)
	if err != nil {
		return typeResolution{}, err
	}
	if cfg == current {
		// Rows 3, 7, 8.
		res := typeResolution{config: cfg}
		if explicit.acceptChangeFrom != "" {
			res.warnings = append(res.warnings, fmt.Sprintf("types.acceptChangeFrom=%q has no effect: the configuration "+
				"equals the position's (%s); remove it", explicit.acceptChangeFrom, current.name()))
		}
		return res, nil
	}

	if phase != phaseCDC {
		// Rows 4 and 9.
		return typeResolution{}, newError(ErrTypesMismatch, CodeTypesSwitchInSnapshot, "types.mode",
			"keep the configuration of the position ("+current.name()+") until the snapshot is done and a change "+
				"event was acknowledged, then change it with types.acceptChangeFrom="+current.name()+"; or create a "+
				"new connector instance with the new configuration (re-snapshot; destinations see duplicates)",
			"the type configuration %s differs from %s, which the position was written with, and the position is "+
				"in the snapshot phase, where switching would mix two schemas inside one table scan", cfg.name(), current.name())
	}
	if explicit.acceptChangeFrom != current.name() {
		// Rows 5, 10, 12.
		stale := ""
		if explicit.acceptChangeFrom != "" {
			stale = fmt.Sprintf(" (types.acceptChangeFrom=%q names another configuration)", explicit.acceptChangeFrom)
		}
		return typeResolution{}, newError(ErrTypesMismatch, CodeTypesStampMismatch, "types.acceptChangeFrom",
			"to change the record shapes from here on, set types.acceptChangeFrom="+current.name()+" and restart; "+
				"keep it until the log says a record under the new configuration was acknowledged. Destinations and "+
				"consumers must accept the new schema version. To keep the old shapes, restore the old settings",
			"the type configuration %s differs from %s, which the position was written with%s", cfg.name(), current.name(), stale)
	}
	// Rows 6 and 11.
	res := typeResolution{config: cfg, switchedFrom: current.name()}
	if !cfg.strict() {
		res.warnings = append(res.warnings, "switching back to legacy type mode: NULL is delivered as the zero value "+
			"and DECIMAL as double from now on")
	}
	return res, nil
}

// completeTypes fills unset settings from the stamp (or the defaults) and
// checks the combination.
func completeTypes(explicit explicitTypes, stamp *common.TypesStamp) (typeConfig, error) {
	base := typesFromStamp(stamp)
	mode := explicit.mode
	if mode == "" {
		mode = base.mode
	}
	if mode == typesModeLegacy {
		if (explicit.decimal != "" && explicit.decimal != typesDecimalDouble) ||
			(explicit.bigintUnsigned != "" && explicit.bigintUnsigned != typesUnsignedLong) {
			path := "types.decimal"
			if explicit.decimal == "" || explicit.decimal == typesDecimalDouble {
				path = "types.bigintUnsigned"
			}
			return typeConfig{}, newError(ErrTypesInvalid, CodeTypesInvalid, path,
				"set types.mode=strict to use "+path+", or remove "+path,
				"legacy mode always maps DECIMAL to double and BIGINT UNSIGNED to long; %s cannot be changed in it", path)
		}
		return legacyTypes, nil
	}
	cfg := typeConfig{mode: typesModeStrict, decimal: typesDecimalString, bigintUnsigned: typesUnsignedLong}
	if base.strict() {
		cfg = base
	}
	if explicit.decimal != "" {
		cfg.decimal = explicit.decimal
	}
	if explicit.bigintUnsigned != "" {
		cfg.bigintUnsigned = explicit.bigintUnsigned
	}
	return cfg, nil
}

// typeSettings is what the source's iterators and schema mappers share at
// run time: the resolved configuration and the phase the next Open would
// see. The zero value is legacy with no position.
type typeSettings struct {
	config typeConfig
	// phase only moves forward: none -> snapshot -> cdc, on acks (see
	// observeAck). It decides which remedy an overflow error names.
	phase atomic.Int32
	// switchedFrom is set while a configuration change has not been
	// acknowledged yet; see observeAck.
	switchedFrom atomic.Pointer[string]

	columns *columnCatalog
}

func newTypeSettings(cfg typeConfig, phase typePhase, switchedFrom string, columns *columnCatalog) *typeSettings {
	t := &typeSettings{config: cfg, columns: columns}
	t.phase.Store(int32(phase))
	if switchedFrom != "" {
		t.switchedFrom.Store(&switchedFrom)
	}
	return t
}

func (t *typeSettings) strict() bool { return t != nil && t.config.strict() }

func (t *typeSettings) stamp() *common.TypesStamp {
	if t == nil {
		return nil
	}
	return t.config.stamp()
}

func (t *typeSettings) currentPhase() typePhase {
	if t == nil {
		return phaseNone
	}
	return typePhase(t.phase.Load())
}

// observeAck moves the phase forward when the source receives an ack: a
// position is persisted no earlier than its ack, so the phase never runs
// ahead of what a restart reads. It returns the stamp being replaced the
// first time a record under a new configuration is acked, so the caller can
// log that types.acceptChangeFrom is no longer needed.
func (t *typeSettings) observeAck(p typePhase) (switchedFrom string) {
	if t == nil {
		return ""
	}
	for {
		cur := t.phase.Load()
		if int32(p) <= cur || t.phase.CompareAndSwap(cur, int32(p)) {
			break
		}
	}
	if from := t.switchedFrom.Swap(nil); from != nil {
		return *from
	}
	return ""
}

// unsignedOverflowError builds the overflow error with the remedy that the
// next Open accepts in the current mode and phase (design: "What legacy is").
func (t *typeSettings) unsignedOverflowError(table, column string, v uint64) *Error {
	what := fmt.Sprintf("column %s.%s holds %d, which is above the largest value its schema type (Avro long) "+
		"can hold (9223372036854775807); the source stopped instead of emitting a wrong value", table, column, v)
	phase := t.currentPhase()
	stampName := legacyStampName
	if t != nil {
		stampName = t.config.name()
	}
	restartHint := " If a record of this connector was acknowledged since it started, restart once: the next " +
		"error names the remedy for the phase the restart reads."
	switch {
	case t.strict() && phase == phaseCDC:
		return newError(ErrUnsignedOverflow, CodeUnsignedOverflow, "types.bigintUnsigned",
			"set types.bigintUnsigned=string and types.acceptChangeFrom="+stampName+", then restart", "%s", what)
	case t.strict():
		return newError(ErrUnsignedOverflow, CodeUnsignedOverflow, "types.bigintUnsigned",
			"create a new connector instance with types.bigintUnsigned=string (re-snapshot; destinations see "+
				"duplicates), exclude the table, or bring the values into range. The setting cannot change in place "+
				"while the pipeline is in its snapshot phase."+restartHint, "%s", what)
	case phase == phaseNone:
		return newError(ErrUnsignedOverflow, CodeUnsignedOverflow, "types.mode",
			"set types.mode=strict and types.bigintUnsigned=string, then restart", "%s", what)
	case phase == phaseCDC:
		return newError(ErrUnsignedOverflow, CodeUnsignedOverflow, "types.mode",
			"set types.mode=strict, types.bigintUnsigned=string and types.acceptChangeFrom=legacy, then restart", "%s", what)
	default:
		return newError(ErrUnsignedOverflow, CodeUnsignedOverflowLegacySnapshot, "types.mode",
			"create a new connector instance with types.mode=strict and types.bigintUnsigned=string (re-snapshot; "+
				"destinations see duplicates), or exclude the table, or bring the values into range. A legacy pipeline "+
				"cannot switch type configuration during its snapshot."+restartHint, "%s", what)
	}
}

// legacyColumnWarnings lists, per column, what legacy mode does not deliver
// exactly (decision 5: one warning per affected column).
func legacyColumnWarnings(tables []string, catalog map[string]map[string]columnInfo) []string {
	var out []string
	for _, table := range tables {
		for _, col := range sortedColumns(catalog[table]) {
			info := catalog[table][col]
			var losses []string
			if info.nullable {
				losses = append(losses, "NULL is delivered as the zero value")
			}
			if info.isDecimal() {
				losses = append(losses, "DECIMAL is delivered as a double and loses precision")
			}
			if len(losses) > 0 {
				out = append(out, fmt.Sprintf("%s: legacy type mode, column %s.%s: %s. Set types.mode=strict to deliver "+
					"it exactly", CodeLegacyTypes, table, col, strings.Join(losses, "; ")))
			}
		}
	}
	return out
}

// bindKey returns a snapshot key value as it must be bound in a resume
// predicate. The driver returns a BIGINT UNSIGNED above 2^63-1 as its digits;
// bound as text, MySQL compares it correctly only while the key is a bare
// column in the predicate (design: "Strict-path resume spike"), so it is bound
// as uint64. Other values are returned unchanged.
func (t *typeSettings) bindKey(table, column string, v any) any {
	b, ok := v.([]byte)
	if !ok || t == nil || t.columns == nil {
		return v
	}
	infos, missing := t.columns.cached(table, []string{column})
	if missing != "" || !infos[column].isUnsignedBigint() {
		return v
	}
	u, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return v
	}
	return u
}
