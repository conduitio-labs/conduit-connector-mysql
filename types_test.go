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
	"errors"
	"testing"

	"github.com/conduitio-labs/conduit-connector-mysql/common"
	"github.com/matryer/is"
)

// TestResolveTypes covers every row of the design's transition table.
func TestResolveTypes(t *testing.T) {
	strictDefault := &common.TypesStamp{Mode: "strict", Decimal: "string", BigintUnsigned: "long"}
	strictS := typeConfig{mode: "strict", decimal: "string", bigintUnsigned: "long"}
	strictU := typeConfig{mode: "strict", decimal: "string", bigintUnsigned: "string"}

	tests := []struct {
		name       string
		explicit   explicitTypes
		positioned bool
		stamp      *common.TypesStamp
		phase      typePhase
		want       typeConfig
		switched   string
		warns      bool
		code       string
	}{
		{name: "row 1: no position, mode unset -> legacy with a warning", want: legacyTypes, warns: true},
		{name: "row 1: another types.* setting without types.mode", explicit: explicitTypes{decimal: "string"}, code: CodeTypesInvalid},
		{name: "row 2: no position, strict", explicit: explicitTypes{mode: "strict"}, want: strictS},
		{name: "row 2: no position, strict with unsigned string", explicit: explicitTypes{mode: "strict", bigintUnsigned: "string"}, want: strictU},
		{name: "row 2: no position, legacy", explicit: explicitTypes{mode: "legacy"}, want: legacyTypes},
		{name: "row 3: legacy position, unset", positioned: true, phase: phaseCDC, want: legacyTypes},
		{name: "row 3: legacy position, legacy", positioned: true, phase: phaseSnapshot, explicit: explicitTypes{mode: "legacy"}, want: legacyTypes},
		{name: "row 4: legacy snapshot, strict", positioned: true, phase: phaseSnapshot, explicit: explicitTypes{mode: "strict", acceptChangeFrom: "legacy"}, code: CodeTypesSwitchInSnapshot},
		{name: "row 5: legacy cdc, strict, no token", positioned: true, phase: phaseCDC, explicit: explicitTypes{mode: "strict"}, code: CodeTypesStampMismatch},
		{name: "row 6: legacy cdc, strict, token", positioned: true, phase: phaseCDC, explicit: explicitTypes{mode: "strict", acceptChangeFrom: "legacy"}, want: strictS, switched: "legacy"},
		{name: "row 7: strict position, unset", positioned: true, stamp: strictDefault, phase: phaseSnapshot, want: strictS},
		{name: "row 7: strict position, only decimal unset", positioned: true, stamp: &common.TypesStamp{Mode: "strict", Decimal: "double", BigintUnsigned: "long"}, phase: phaseCDC, explicit: explicitTypes{mode: "strict"}, want: typeConfig{mode: "strict", decimal: "double", bigintUnsigned: "long"}},
		{name: "row 8: strict position, equal", positioned: true, stamp: strictDefault, phase: phaseCDC, explicit: explicitTypes{mode: "strict", decimal: "string", bigintUnsigned: "long"}, want: strictS},
		{name: "row 8: leftover token warns", positioned: true, stamp: strictDefault, phase: phaseCDC, explicit: explicitTypes{mode: "strict", acceptChangeFrom: "legacy"}, want: strictS, warns: true},
		{name: "row 9: strict snapshot, differs", positioned: true, stamp: strictDefault, phase: phaseSnapshot, explicit: explicitTypes{bigintUnsigned: "string", acceptChangeFrom: "strict/string/long"}, code: CodeTypesSwitchInSnapshot},
		{name: "row 10: strict cdc, differs, no token", positioned: true, stamp: strictDefault, phase: phaseCDC, explicit: explicitTypes{bigintUnsigned: "string"}, code: CodeTypesStampMismatch},
		{name: "row 11: strict cdc, differs, token", positioned: true, stamp: strictDefault, phase: phaseCDC, explicit: explicitTypes{bigintUnsigned: "string", acceptChangeFrom: "strict/string/long"}, want: strictU, switched: "strict/string/long"},
		{name: "row 11: back to legacy, warned", positioned: true, stamp: strictDefault, phase: phaseCDC, explicit: explicitTypes{mode: "legacy", acceptChangeFrom: "strict/string/long"}, want: legacyTypes, switched: "strict/string/long", warns: true},
		{name: "row 12: stale token", positioned: true, stamp: strictDefault, phase: phaseCDC, explicit: explicitTypes{bigintUnsigned: "string", acceptChangeFrom: "legacy"}, code: CodeTypesStampMismatch},
		{name: "legacy cannot change decimal", explicit: explicitTypes{mode: "legacy", decimal: "string"}, code: CodeTypesInvalid},
		{name: "decimal logical type is cut from this version", explicit: explicitTypes{mode: "strict", decimal: "decimal"}, code: CodeTypesInvalid},
		{name: "unknown mode", explicit: explicitTypes{mode: "loose"}, code: CodeTypesInvalid},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			got, err := resolveTypes(tc.explicit, tc.positioned, tc.stamp, tc.phase)
			if tc.code != "" {
				var e *Error
				is.True(errors.As(err, &e))
				is.Equal(e.Code, tc.code)
				return
			}
			is.NoErr(err)
			is.Equal(got.config, tc.want)
			is.Equal(got.switchedFrom, tc.switched)
			is.Equal(len(got.warnings) > 0, tc.warns)
		})
	}
}

// Each overflow remedy must be one the next Open accepts (the design's
// "stranded pipeline" rule).
func TestUnsignedOverflowRemedies(t *testing.T) {
	strict := typeConfig{mode: "strict", decimal: "string", bigintUnsigned: "long"}
	tests := []struct {
		name     string
		cfg      typeConfig
		phase    typePhase
		code     string
		accepted *explicitTypes // the settings the remedy names; nil: needs a new connector instance
	}{
		{
			"legacy, no position", legacyTypes, phaseNone, CodeUnsignedOverflow,
			&explicitTypes{mode: "strict", bigintUnsigned: "string"},
		},
		{
			"legacy, cdc", legacyTypes, phaseCDC, CodeUnsignedOverflow,
			&explicitTypes{mode: "strict", bigintUnsigned: "string", acceptChangeFrom: "legacy"},
		},
		{"legacy, snapshot", legacyTypes, phaseSnapshot, CodeUnsignedOverflowLegacySnapshot, nil},
		{
			"strict, cdc", strict, phaseCDC, CodeUnsignedOverflow,
			&explicitTypes{mode: "strict", bigintUnsigned: "string", acceptChangeFrom: "strict/string/long"},
		},
		{"strict, snapshot", strict, phaseSnapshot, CodeUnsignedOverflow, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ts := newTypeSettings(tc.cfg, tc.phase, "", nil)
			e := ts.unsignedOverflowError("t", "u", 18446744073709551615)
			is.Equal(e.Code, tc.code)
			is.True(errors.Is(e, ErrUnsignedOverflow))
			if tc.accepted == nil {
				return
			}
			positioned := tc.phase != phaseNone
			res, err := resolveTypes(*tc.accepted, positioned, tc.cfg.stamp(), tc.phase)
			is.NoErr(err) // the remedy must not be refused by the next Open
			is.Equal(res.config.bigintUnsigned, "string")
		})
	}
}

func TestTypeSettingsPhaseOnlyMovesForward(t *testing.T) {
	is := is.New(t)
	ts := newTypeSettings(legacyTypes, phaseNone, "legacy", nil)
	is.Equal(ts.observeAck(phaseSnapshot), "legacy") // first ack after a switch reports it once
	is.Equal(ts.observeAck(phaseCDC), "")
	ts.observeAck(phaseSnapshot)
	is.Equal(ts.currentPhase(), phaseCDC)
}

func TestFormatStrict(t *testing.T) {
	is := is.New(t)
	m := newSchemaMapper(newTypeSettings(typeConfig{mode: "strict", decimal: "string", bigintUnsigned: "string"}, phaseNone, "", nil))
	m.table = "t"
	m.colTypes = map[string]*avroNamedType{
		"n":   {Name: "n", strict: &strictColumn{nullable: true}},
		"nn":  {Name: "nn", strict: &strictColumn{}},
		"dec": {Name: "dec", strict: &strictColumn{decimalString: true, columnType: "decimal(30,10)"}},
		"u":   {Name: "u", strict: &strictColumn{unsignedString: true, columnType: "bigint unsigned"}},
	}
	v, err := m.formatValue(t.Context(), "n", nil)
	is.NoErr(err)
	is.Equal(v, nil)

	_, err = m.formatValue(t.Context(), "nn", nil)
	is.True(errors.Is(err, ErrNullInNotNull))

	v, err = m.formatValue(t.Context(), "dec", []byte("12345678901234567890.1234567891"))
	is.NoErr(err)
	is.Equal(v, "12345678901234567890.1234567891")
	v, err = m.formatValue(t.Context(), "dec", "0.00")
	is.NoErr(err)
	is.Equal(v, "0.00")

	for _, in := range []any{uint64(18446744073709551615), []byte("18446744073709551615")} {
		v, err = m.formatValue(t.Context(), "u", in)
		is.NoErr(err)
		is.Equal(v, "18446744073709551615")
	}
	v, err = m.formatValue(t.Context(), "u", int64(7))
	is.NoErr(err)
	is.Equal(v, "7")
	_, err = m.formatValue(t.Context(), "u", []byte("x1"))
	is.True(errors.Is(err, ErrValueNotRepresentable))
}

func TestPositionTypesStamp(t *testing.T) {
	is := is.New(t)
	stamp := &common.TypesStamp{Mode: "strict", Decimal: "string", BigintUnsigned: "long"}
	is.Equal(stamp.Name(), "strict/string/long")
	is.Equal((*common.TypesStamp)(nil).Name(), "legacy")

	pos := common.CdcPosition{ReplicationEventPosition: common.ReplicationEventPosition{Name: "binlog.000001", Pos: 9}}
	parsed, err := common.ParseSDKPosition(pos.ToSDKPositionWithTypes(stamp))
	is.NoErr(err)
	is.Equal(parsed.Version, common.TypesPositionVersion)
	is.Equal(*parsed.Types, *stamp)

	legacy, err := common.ParseSDKPosition(pos.ToSDKPosition())
	is.NoErr(err)
	is.Equal(legacy.Version, common.CDCPositionVersion)
	is.True(legacy.Types == nil)

	// A v3 envelope without its stamp is refused rather than resumed as legacy.
	_, err = common.ParseSDKPosition([]byte(`{"version":3,"cdc_position":{"name":"binlog.000001","pos":9}}`))
	is.True(errors.Is(err, common.ErrPositionVersionUnsupported))
}
