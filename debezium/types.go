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

package debezium

import (
	"strconv"
	"strings"
)

// NormalizeType reduces a MySQL type name to its base type and unsigned flag.
// It accepts the forms both decode paths produce:
//
//   - the driver's uppercase DatabaseTypeName, e.g. "UNSIGNED BIGINT", "INT",
//     "VARCHAR";
//   - go-mysql's RawType, e.g. "int unsigned", "decimal(10,2)", "varchar(255)".
//
// It lowercases, detects the UNSIGNED qualifier (both the driver's "UNSIGNED
// BIGINT" prefix and go-mysql's "int unsigned" suffix), and strips any "(...)"
// suffix (numeric display widths, decimal precision/scale, string lengths,
// temporal precision, enum/set members). A BIT(M) display width, which changes
// the mapping (BIT(1) is boolean, BIT(>1) is bytes), is not part of the base
// type; callers must pass Precision separately. An empty or unknown result is
// the caller's to reject with UnknownTypeError via MapColumn.
func NormalizeType(raw string) (base string, unsigned bool) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.HasPrefix(raw, "unsigned "):
		unsigned = true
		raw = strings.TrimPrefix(raw, "unsigned ")
	case strings.HasSuffix(raw, " unsigned"):
		unsigned = true
		raw = strings.TrimSuffix(raw, " unsigned")
	}
	if i := strings.IndexByte(raw, '('); i >= 0 {
		raw = raw[:i]
	}
	return strings.TrimSpace(raw), unsigned
}

// MapColumn maps one MySQL column to its Connect field under the given handling
// modes. It is total over the supported base types and returns UnknownTypeError
// otherwise; the caller must never substitute a guessed field.
func MapColumn(col Column, opts Options) (Field, error) {
	f := Field{
		Name:     col.Name,
		Optional: col.Nullable,
		Default:  col.Default,
	}

	// The mapping table from docs/design-documents/20261016-debezium-compatible-output.md,
	// in the order Debezium's own documentation lists it.
	switch col.BaseType {
	case "bool", "boolean":
		f.Type = TypeBoolean

	case "tinyint":
		f.Type = TypeInt16
	case "smallint":
		if col.Unsigned {
			f.Type = TypeInt32
		} else {
			f.Type = TypeInt16
		}
	case "mediumint":
		f.Type = TypeInt32
	case "int", "integer":
		if col.Unsigned {
			f.Type = TypeInt64
		} else {
			f.Type = TypeInt32
		}
	case "bigint":
		switch {
		case col.Unsigned && opts.BigIntUnsignedHandlingMode == BigIntUnsignedString:
			f.Type = TypeString
		case col.Unsigned && opts.BigIntUnsignedHandlingMode == BigIntUnsignedPrecise:
			f.Type = TypeBytes
			f.Semantic = SemanticDecimal
			f.Parameters = map[string]string{"scale": "0", "connect.decimal.precision": "20"}
		default:
			f.Type = TypeInt64
		}

	case "real":
		f.Type = TypeFloat32
	case "float", "double":
		f.Type = TypeFloat64

	case "decimal", "numeric":
		switch opts.DecimalHandlingMode {
		case DecimalDouble:
			f.Type = TypeFloat64
		case DecimalString:
			f.Type = TypeString
		default:
			f.Type = TypeBytes
			f.Semantic = SemanticDecimal
			f.Parameters = map[string]string{"scale": strconv.Itoa(col.Scale)}
			if col.Precision > 0 {
				f.Parameters["connect.decimal.precision"] = strconv.Itoa(col.Precision)
			}
		}

	case "bit":
		if col.Precision == 1 {
			f.Type = TypeBoolean
		} else {
			f.Type, f.Semantic = TypeBytes, SemanticBits
			switch opts.BinaryHandlingMode {
			case BinaryBase64, BinaryHex:
				f.Type = TypeString
				f.Semantic = ""
			}
			if col.Precision > 1 {
				f.Parameters = map[string]string{"length": strconv.Itoa(col.Precision)}
			}
		}

	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		f.Type = TypeString
	case "enum":
		f.Type, f.Semantic = TypeString, SemanticEnum
	case "set":
		f.Type, f.Semantic = TypeString, SemanticEnumSet
	case "json":
		f.Type, f.Semantic = TypeString, SemanticJSON

	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		f.Type = TypeBytes
		if opts.BinaryHandlingMode == BinaryBase64 || opts.BinaryHandlingMode == BinaryHex {
			f.Type = TypeString
		}

	case "date":
		f.Type = TypeInt32
		f.Semantic = SemanticDate
		if opts.TimePrecisionMode == TimePrecisionConnect {
			f.Semantic = SemanticConnectDate
		}
	case "time":
		f.Type = TypeInt64
		f.Semantic = SemanticMicroTime
		if opts.TimePrecisionMode == TimePrecisionConnect {
			f.Semantic = SemanticConnectTime
		}
	case "datetime":
		f.Type = TypeInt64
		f.Semantic = SemanticTimestamp
		if opts.TimePrecisionMode == TimePrecisionConnect {
			f.Semantic = SemanticConnectTimestamp
		} else if col.Precision >= 4 {
			f.Semantic = SemanticMicroTimestamp
		}
	case "timestamp":
		f.Type, f.Semantic = TypeString, SemanticZonedTimestamp
	case "year":
		f.Type, f.Semantic = TypeInt32, SemanticYear

	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
		// Spatial columns map to a Geometry struct; the value formatter builds
		// {srid, wkb}. Not reached until canal surfaces spatial columns.
		f.Type, f.Semantic = TypeStruct, SemanticGeometry

	default:
		return Field{}, UnknownTypeError{Column: col.Name, Type: col.BaseType}
	}

	return f, nil
}

// FromRawType builds a Column from a raw type name, its unsigned flag and a
// name. It is a convenience for the decode paths that only have the raw string;
// callers that have real precision/scale/nullability must set them after.
func FromRawType(name, rawType string, unsigned bool) Column {
	base, _ := NormalizeType(rawType)
	return Column{Name: name, BaseType: base, Unsigned: unsigned}
}
