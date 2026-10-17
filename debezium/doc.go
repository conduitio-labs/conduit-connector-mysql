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

// Package debezium maps MySQL columns and row values to the Kafka Connect
// types that Debezium's MySQL connector (io.debezium.connector.mysql) emits.
//
// It is the type-mapping half of the Debezium-compatible MySQL CDC output mode
// described in docs/design-documents/20261016-debezium-compatible-output.md. It
// is deliberately a pure, self-contained package with no database access and no
// I/O: MapColumn is a function of column metadata plus handling-mode options,
// and FormatValue is a function of a decoded value plus the mapped field. The
// snapshot and CDC iterators will feed it; they do not today.
//
// The mapping follows Debezium's MySQL type mapping
// (c_how-the-mysql-connector-maps-data-types.adoc) with the default handling
// modes: decimal.handling.mode=precise,
// time.precision.mode=adaptive_time_microseconds, binary.handling.mode=bytes,
// bigint.unsigned.handling.mode=long.
//
// # Invariants
//
//   - Lossless by default: NULL stays null, DECIMAL is a decimal string (never
//     a float), and BIGINT UNSIGNED above 2^63-1 fails with UnsignedOverflowError
//     in long mode rather than wrapping (invariant 6). Nothing here coerces a
//     value into a narrower one silently.
//   - MapColumn is total over the supported base types and returns a
//     descriptive error (not a panic) for an unknown type, so an unexpected
//     column type fails loudly instead of being emitted with a wrong shape.
package debezium

// Type is a Kafka Connect literal (physical) type.
type Type string

// The Kafka Connect literal types the MySQL mapping produces.
const (
	TypeBoolean Type = "boolean"
	TypeInt16   Type = "int16"
	TypeInt32   Type = "int32"
	TypeInt64   Type = "int64"
	TypeFloat32 Type = "float32"
	TypeFloat64 Type = "float64"
	TypeString  Type = "string"
	TypeBytes   Type = "bytes"
	TypeStruct  Type = "struct"
)

// Semantic (logical) type names, as Debezium names them.
const (
	SemanticDecimal          = "org.apache.kafka.connect.data.Decimal"
	SemanticDate             = "io.debezium.time.Date"
	SemanticMicroTime        = "io.debezium.time.MicroTime"
	SemanticTimestamp        = "io.debezium.time.Timestamp"
	SemanticMicroTimestamp   = "io.debezium.time.MicroTimestamp"
	SemanticZonedTimestamp   = "io.debezium.time.ZonedTimestamp"
	SemanticYear             = "io.debezium.time.Year"
	SemanticJSON             = "io.debezium.data.Json"
	SemanticEnum             = "io.debezium.data.Enum"
	SemanticEnumSet          = "io.debezium.data.EnumSet"
	SemanticBits             = "io.debezium.data.Bits"
	SemanticGeometry         = "io.debezium.data.geometry.Geometry"
	SemanticConnectDate      = "org.apache.kafka.connect.data.Date"
	SemanticConnectTime      = "org.apache.kafka.connect.data.Time"
	SemanticConnectTimestamp = "org.apache.kafka.connect.data.Timestamp"
)

// Handling-mode values, mirroring Debezium's connector settings.
const (
	DecimalPrecise = "precise"
	DecimalDouble  = "double"
	DecimalString  = "string"

	BigIntUnsignedLong    = "long"
	BigIntUnsignedPrecise = "precise"
	BigIntUnsignedString  = "string"

	TimePrecisionAdaptive = "adaptive_time_microseconds"
	TimePrecisionConnect  = "connect"

	BinaryBytes  = "bytes"
	BinaryBase64 = "base64"
	BinaryHex    = "hex"
)

// Options selects the handling-mode behavior of MapColumn. Zero values are not
// valid; callers must set every field (DefaultOptions returns the Debezium
// defaults).
type Options struct {
	// DecimalHandlingMode is one of DecimalPrecise, DecimalDouble, DecimalString.
	DecimalHandlingMode string
	// BigIntUnsignedHandlingMode is one of BigIntUnsignedLong,
	// BigIntUnsignedPrecise, BigIntUnsignedString.
	BigIntUnsignedHandlingMode string
	// TimePrecisionMode is one of TimePrecisionAdaptive, TimePrecisionConnect.
	TimePrecisionMode string
	// BinaryHandlingMode is one of BinaryBytes, BinaryBase64, BinaryHex.
	BinaryHandlingMode string
}

// DefaultOptions returns the Debezium MySQL defaults for the handling modes.
func DefaultOptions() Options {
	return Options{
		DecimalHandlingMode:        DecimalPrecise,
		BigIntUnsignedHandlingMode: BigIntUnsignedLong,
		TimePrecisionMode:          TimePrecisionAdaptive,
		BinaryHandlingMode:         BinaryBytes,
	}
}

// Column describes a MySQL column with the metadata the mapping needs. It is
// the neutral input both the snapshot path (go-sql-driver ColumnTypes) and the
// CDC path (go-mysql schema.TableColumn) populate.
type Column struct {
	// Name is the column name.
	Name string
	// BaseType is the normalized base type, lowercased, without a length or
	// "(M[,D])" suffix and without an "unsigned" prefix (see NormalizeType):
	// "tinyint", "int", "bigint", "decimal", "varchar", "bit", "date", ...
	BaseType string
	// Unsigned reports an UNSIGNED integer column.
	Unsigned bool
	// Precision is the fractional-seconds precision p of TIME/DATETIME (0 to
	// 6), the display width M of BIT (BIT(M)), or the total digits M of DECIMAL.
	// 0 means unspecified, which the mapping treats as the column's default.
	Precision int
	// Scale is the scale s of DECIMAL(p,s).
	Scale int
	// Nullable reports whether the column may hold NULL.
	Nullable bool
	// Default is the column's literal default, if any; nil means none.
	Default any
}

// Field is the Connect schema of one column: its physical type, optional
// semantic type, and the parameters that belong in the schema.
type Field struct {
	// Name is the field (column) name.
	Name string
	// Type is the Connect literal type.
	Type Type
	// Semantic is the logical type name, or "" for a plain type.
	Semantic string
	// Parameters holds logical-type parameters (for example "scale" for
	// Decimal, "length" for Bits, "allowed" for Enum/EnumSet).
	Parameters map[string]string
	// Optional mirrors the column's nullability.
	Optional bool
	// Default is the column's literal default, if any.
	Default any
}

// UnknownTypeError reports a column whose base type MapColumn does not know. The
// caller must surface it, never fall back to a guessed type: a guessed type
// would emit a wrong schema silently (invariant 6).
type UnknownTypeError struct {
	Column string
	Type   string
}

func (e UnknownTypeError) Error() string {
	return "unsupported MySQL type " + e.Type + " for column " + e.Column
}
