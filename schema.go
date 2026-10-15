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

package mysql

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/iskorotkov/avro/v2"
	"github.com/jmoiron/sqlx"
)

// schemaMapper maps mysql schemas to conduit avro schemas, and it uses them to
// format values.
type schemaMapper struct {
	schema   *schemaSubjectVersion
	colTypes map[string]*avroNamedType
	// table is the table the schema describes, for error messages.
	table string
	// types is the source's type configuration; nil is legacy.
	types *typeSettings
}

func newSchemaMapper(types *typeSettings) *schemaMapper {
	return &schemaMapper{
		colTypes: make(map[string]*avroNamedType),
		types:    types,
	}
}

type avroType struct {
	Type   avro.Type
	isBit  bool
	isDate bool
}

var sqlColtypeToAvroTypeMap = map[string]avroType{
	// Numeric types
	"TINYINT":   {Type: avro.Int},
	"SMALLINT":  {Type: avro.Int},
	"MEDIUMINT": {Type: avro.Int},
	"INT":       {Type: avro.Int},
	"BIGINT":    {Type: avro.Long},

	"UNSIGNED TINYINT":   {Type: avro.Int},
	"UNSIGNED SMALLINT":  {Type: avro.Int},
	"UNSIGNED MEDIUMINT": {Type: avro.Int},
	"UNSIGNED INT":       {Type: avro.Long},
	"UNSIGNED BIGINT":    {Type: avro.Long},

	"FLOAT":   {Type: avro.Float},
	"DECIMAL": {Type: avro.Double},
	"NUMERIC": {Type: avro.Double},
	"DOUBLE":  {Type: avro.Double},
	"BIT":     {Type: avro.Fixed, isBit: true},

	// String types
	"CHAR":       {Type: avro.String},
	"VARCHAR":    {Type: avro.String},
	"TINYTEXT":   {Type: avro.String},
	"TEXT":       {Type: avro.String},
	"MEDIUMTEXT": {Type: avro.String},
	"LONGTEXT":   {Type: avro.String},

	// Binary types
	"BINARY":     {Type: avro.Bytes},
	"VARBINARY":  {Type: avro.Bytes},
	"TINYBLOB":   {Type: avro.Bytes},
	"BLOB":       {Type: avro.Bytes},
	"MEDIUMBLOB": {Type: avro.Bytes},
	"LONGBLOB":   {Type: avro.Bytes},

	// Date and type types
	"DATE":      {Type: avro.String, isDate: true},
	"TIME":      {Type: avro.String, isDate: true},
	"DATETIME":  {Type: avro.String, isDate: true},
	"TIMESTAMP": {Type: avro.String, isDate: true},
	"YEAR":      {Type: avro.Int, isDate: true},

	// Misc
	"ENUM": {Type: avro.String},
	"SET":  {Type: avro.String},
	"JSON": {Type: avro.String},
}

type avroNamedType struct {
	avroType
	Name string

	// dbType is the driver's type name for the column, lowercase and without
	// "unsigned". Empty when the column did not come from a query.
	dbType string

	// strict is set in types.mode=strict, from information_schema; nil in
	// legacy mode.
	strict *strictColumn
}

// strictColumn is how strict mode maps a column. See ConduitIO/conduit
// docs/design-documents/20261012-mysql-type-semantics.md.
type strictColumn struct {
	// nullable makes the field the union ["null", T] with default null. Key
	// columns are never nullable.
	nullable bool
	// columnType is information_schema COLUMN_TYPE, written to the field's
	// "mysql.type" property for exactly mapped DECIMAL and unsigned columns.
	columnType string
	// decimalString: DECIMAL/NUMERIC as its exact text (types.decimal=string).
	decimalString bool
	// unsignedString: BIGINT UNSIGNED as decimal text (types.bigintUnsigned=string).
	unsignedString bool
}

// refineStrict returns copies of cols mapped for strict mode. In legacy mode
// it returns cols unchanged.
func (s *schemaMapper) refineStrict(ctx context.Context, table string, cols []*avroNamedType, key bool) ([]*avroNamedType, error) {
	if !s.types.strict() {
		return cols, nil
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	infos, err := s.types.columns.lookup(ctx, table, names)
	if err != nil {
		return nil, err
	}
	out := make([]*avroNamedType, len(cols))
	for i, c := range cols {
		refined := *c
		info := infos[c.Name]
		st := &strictColumn{nullable: info.nullable && !key, columnType: info.columnType}
		switch {
		case info.isDecimal() && s.types.config.decimal == typesDecimalString:
			refined.Type = avro.String
			st.decimalString = true
		case info.isDecimal():
			refined.Type = avro.Double
		case info.isUnsignedBigint() && s.types.config.bigintUnsigned == typesUnsignedString:
			refined.Type = avro.String
			st.unsignedString = true
		}
		refined.strict = st
		out[i] = &refined
	}
	return out, nil
}

func sqlxRowsToAvroCol(rows *sqlx.Rows) ([]*avroNamedType, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column types: %w", err)
	}

	avroCols := make([]*avroNamedType, len(colTypes))
	for i, colType := range colTypes {
		avroType, ok := sqlColtypeToAvroTypeMap[colType.DatabaseTypeName()]
		if !ok {
			return nil, fmt.Errorf(
				"failed to retrieve column type %s for %s",
				colType.DatabaseTypeName(), colType.Name())
		}

		avroCol := &avroNamedType{
			avroType: avroType,
			Name:     colType.Name(),
			dbType:   strings.TrimPrefix(strings.ToLower(colType.DatabaseTypeName()), "unsigned "),
		}
		if colType.DatabaseTypeName() == "BIT" {
			avroCol.isBit = true
		}

		avroCols[i] = avroCol
	}

	return avroCols, nil
}

var mysqlschemaTypeToAvroTypeMap = map[int]avroType{
	// Numeric types
	mysqlschema.TYPE_NUMBER:     {Type: avro.Int},
	mysqlschema.TYPE_FLOAT:      {Type: avro.Float},
	mysqlschema.TYPE_DECIMAL:    {Type: avro.Double},
	mysqlschema.TYPE_MEDIUM_INT: {Type: avro.Int},

	// String types
	mysqlschema.TYPE_STRING: {Type: avro.String},
	mysqlschema.TYPE_ENUM:   {Type: avro.String},
	mysqlschema.TYPE_SET:    {Type: avro.String},

	// Binary types
	mysqlschema.TYPE_BINARY: {Type: avro.Bytes},
	mysqlschema.TYPE_BIT:    {Type: avro.Fixed, isBit: true},

	// Date and time types
	mysqlschema.TYPE_DATETIME:  {Type: avro.String, isDate: true},
	mysqlschema.TYPE_TIMESTAMP: {Type: avro.String, isDate: true},
	mysqlschema.TYPE_DATE:      {Type: avro.String, isDate: true},
	mysqlschema.TYPE_TIME:      {Type: avro.String, isDate: true},

	// Misc
	mysqlschema.TYPE_JSON:  {Type: avro.String},
	mysqlschema.TYPE_POINT: {Type: avro.String},
}

var rawTypeToAvroTypeMap = map[string]avro.Type{
	"bigint":          avro.Long,
	"bigint unsigned": avro.Long,
	"tinyblob":        avro.Bytes,
	"blob":            avro.Bytes,
	"mediumblob":      avro.Bytes,
	"longblob":        avro.Bytes,
}

var rawTypeRe = regexp.MustCompile(`(\([^)]+\))`)

func mysqlSchemaToAvroCol(tableCol mysqlschema.TableColumn) (*avroNamedType, error) {
	avroType, ok := mysqlschemaTypeToAvroTypeMap[tableCol.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported column type %s for column %s", tableCol.RawType, tableCol.Name)
	}

	// Numeric type names could contain the total number of digits that can be stored.
	// Remove any value in parenthesis to understand real type.
	// https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html
	rawType := rawTypeRe.ReplaceAllString(tableCol.RawType, "")

	if rawType == "int unsigned" {
		avroType.Type = avro.Long
	}

	if tableCol.Type == mysqlschema.TYPE_FLOAT && rawType == "double" {
		avroType.Type = avro.Double
	}

	avroColType := &avroNamedType{avroType: avroType, Name: tableCol.Name}
	finalType, ok := rawTypeToAvroTypeMap[rawType]
	if ok {
		avroColType.Type = finalType
	}

	return avroColType, nil
}

func colTypeToAvroField(avroCol *avroNamedType) (*avro.Field, error) {
	var typ avro.Schema
	if avroCol.isBit {
		// Current limitations in the mysql driver that we use don't allow use
		// to get the N from BIT(N) mysql columns. To track support for this
		// feature refer to https://github.com/go-sql-driver/mysql/issues/1672
		fixed8Size := 8

		fixed, err := avro.NewFixedSchema(avroCol.Name+"_fixed", "", fixed8Size, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create fixed schema for bit column %s: %w", avroCol.Name, err)
		}
		typ = fixed
	} else {
		typ = avro.NewPrimitiveSchema(avroCol.Type, nil)
	}

	if avroCol.strict == nil {
		field, err := avro.NewField(avroCol.Name, typ)
		if err != nil {
			return nil, fmt.Errorf("failed to create avro field for column %s: %w", avroCol.Name, err)
		}
		return field, nil
	}

	var opts []avro.SchemaOption
	if avroCol.strict.decimalString || avroCol.strict.unsignedString {
		// Advisory: precision and scale survive in the registry, but a
		// processor that re-encodes the record drops properties.
		opts = append(opts, avro.WithProps(map[string]any{"mysql.type": avroCol.strict.columnType}))
	}
	if avroCol.strict.nullable {
		union, err := avro.NewUnionSchema([]avro.Schema{avro.NewPrimitiveSchema(avro.Null, nil), typ})
		if err != nil {
			return nil, fmt.Errorf("failed to create nullable type for column %s: %w", avroCol.Name, err)
		}
		typ = union
		opts = append(opts, avro.WithDefault(nil))
	}
	field, err := avro.NewField(avroCol.Name, typ, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro field for column %s: %w", avroCol.Name, err)
	}
	return field, nil
}

type schemaSubjectVersion struct {
	subject string
	version int
}

func (s *schemaMapper) createPayloadSchema(
	ctx context.Context, schemaName string, colTypes []*avroNamedType,
) (*schemaSubjectVersion, error) {
	s.table = schemaName
	if s.schema != nil {
		return s.schema, nil
	}
	colTypes, err := s.refineStrict(ctx, schemaName, colTypes, false)
	if err != nil {
		return nil, err
	}
	return s.createSchema(ctx, schemaName+"_payload", colTypes)
}

func (s *schemaMapper) createKeySchema(
	ctx context.Context, schemaName string, colTypes []*avroNamedType,
) (*schemaSubjectVersion, error) {
	s.table = schemaName
	if s.schema != nil {
		return s.schema, nil
	}
	colTypes, err := s.refineStrict(ctx, schemaName, colTypes, true)
	if err != nil {
		return nil, err
	}
	return s.createSchema(ctx, schemaName+"_key", colTypes)
}

// buildRecordSchema is the record schema createSchema registers for a table.
// It is a function of its own so the Avro compatibility test pins exactly what
// is registered (name, namespace, field order), not a copy of it.
func buildRecordSchema(table string, mysqlCols []*avroNamedType) (*avro.RecordSchema, error) {
	fields := make([]*avro.Field, 0, len(mysqlCols))
	for _, colType := range mysqlCols {
		field, err := colTypeToAvroField(colType)
		if err != nil {
			return nil, fmt.Errorf("failed to create payload schema: %w", err)
		}

		fields = append(fields, field)
	}

	recordSchema, err := avro.NewRecordSchema(table, "mysql", fields)
	if err != nil {
		return nil, fmt.Errorf("failed to create payload schema: %w", err)
	}

	return recordSchema, nil
}

func (s *schemaMapper) createSchema(
	ctx context.Context, table string, mysqlCols []*avroNamedType,
) (*schemaSubjectVersion, error) {
	if s.schema != nil {
		return s.schema, nil
	}

	recordSchema, err := buildRecordSchema(table, mysqlCols)
	if err != nil {
		return nil, err
	}
	for _, colType := range mysqlCols {
		s.colTypes[colType.Name] = colType
	}

	// Legacy registers the canonical form, as earlier versions did, so an
	// upgrade does not register a new schema version. Strict registers the
	// full form: the canonical one drops field defaults and properties.
	text := []byte(recordSchema.String())
	if s.types.strict() {
		if text, err = json.Marshal(recordSchema); err != nil {
			return nil, fmt.Errorf("failed to encode payload schema: %w", err)
		}
	}

	schema, err := schema.Create(ctx, schema.TypeAvro, recordSchema.Name(), text)
	if err != nil {
		return nil, fmt.Errorf("failed to create payload schema: %w", err)
	}

	s.schema = &schemaSubjectVersion{
		subject: schema.Subject,
		version: schema.Version,
	}

	return s.schema, nil
}

// formatValue uses the stored avro types to format the incoming value as an
// avro type. This way we can better control what the mysql driver returns. It
// should be called after creating the schema, otherwise it won't do anything.
//
// It returns an error, and no value, for a value the column's schema type
// cannot hold (see CodeUnsignedOverflow). The caller must not emit the record.
func (s *schemaMapper) formatValue(ctx context.Context, column string, value any) (any, error) {
	if t, ok := s.colTypes[column]; ok && t.strict != nil {
		return s.formatStrict(ctx, column, t, value)
	}
	return s.formatLegacy(ctx, column, value)
}

// formatStrict formats a value in strict mode.
//
// Invariant 6: a value is emitted exactly or the source stops with a coded
// error; NULL is null, DECIMAL text is kept, nothing is coerced to a zero
// value or wrapped.
func (s *schemaMapper) formatStrict(ctx context.Context, column string, t *avroNamedType, value any) (any, error) {
	if value == nil {
		if t.strict.nullable {
			return nil, nil //nolint:nilnil // a SQL NULL in a nullable column is a nil value
		}
		return nil, newError(ErrNullInNotNull, CodeNullInNotNull, "tables",
			"check the column definition, and that every writer logs full row images (binlog_row_image=FULL)",
			"column %s.%s is NOT NULL (or part of the key) but a NULL arrived; it was not emitted as a zero value",
			s.table, column)
	}
	notRepresentable := func(why string) error {
		return newError(ErrValueNotRepresentable, CodeValueNotRepresentable, "tables",
			"exclude the table, or report the column type and value if the column type is supported",
			"column %s.%s (%s): %s; the source stopped instead of coercing it", s.table, column, t.strict.columnType, why)
	}
	switch {
	case t.strict.decimalString:
		switch v := value.(type) {
		case []byte:
			return string(v), nil
		case string:
			return v, nil
		}
		return nil, notRepresentable(fmt.Sprintf("unexpected %T for DECIMAL text", value))
	case t.strict.unsignedString:
		switch v := value.(type) {
		case int64:
			if v < 0 {
				return nil, notRepresentable("negative value for BIGINT UNSIGNED")
			}
			return strconv.FormatInt(v, 10), nil
		case uint64:
			return strconv.FormatUint(v, 10), nil
		case []byte, string:
			// []byte from the driver; string when an already formatted value
			// is formatted again (CDC keys are built from the payload).
			text := fmt.Sprint(v)
			if b, ok := v.([]byte); ok {
				text = string(b)
			}
			if _, err := strconv.ParseUint(text, 10, 64); err != nil {
				return nil, notRepresentable(fmt.Sprintf("BIGINT UNSIGNED text %q does not parse", text))
			}
			return text, nil
		}
		return nil, notRepresentable(fmt.Sprintf("unexpected %T for BIGINT UNSIGNED", value))
	}

	out, err := s.formatLegacy(ctx, column, value)
	if err != nil {
		return nil, err
	}
	// The legacy formatting passes some values on unconverted (out of range,
	// unparseable); in strict mode that is a halt, not an encoder error later.
	switch t.Type { //nolint:exhaustive // only these branches can pass a value on unconverted
	case avro.Int:
		if _, ok := out.(int32); !ok {
			return nil, notRepresentable(fmt.Sprintf("%v does not fit an Avro int", value))
		}
	case avro.Long:
		if _, ok := out.(int64); !ok {
			return nil, notRepresentable(fmt.Sprintf("%v does not fit an Avro long", value))
		}
	case avro.Double:
		if _, ok := out.(float64); !ok {
			return nil, notRepresentable(fmt.Sprintf("%v does not parse as a double", value))
		}
	}
	return out, nil
}

// formatLegacy is the formatting of earlier versions, used as is in legacy
// mode and for non-null values of the types strict mode does not change.
func (s *schemaMapper) formatLegacy(ctx context.Context, column string, value any) (any, error) {
	t, found := s.colTypes[column]
	if !found {
		// In snapshot mode, this should never happen.
		// In CDC mode, to prevent getting into here we make sure to instantiate the
		// schema mapper for each row event.
		sdk.Logger(ctx).Warn().Msgf("column \"%v\" not found", column)
		return value, nil
	}

	// Each of the following branches handles different datatype parsing
	// behaviour between database/sql and go-mysql-org/go-mysql/canal
	// dependencies. We need this to make sure that a row emitted in snapshot mode
	// and updated in cdc mode have the same schema.

	// We manually convert nil values into the go zero value equivalent so that
	// we don't need to handle NULL complexity into the schema.
	// However, we might want to reflect nullability of the datatype in the future.
	if value == nil {
		return defaultValueForType(t.Type), nil
	}

	switch t.Type {
	case avro.String:
		switch v := value.(type) {
		case []uint8:
			return string(v), nil
		case string:
			if !t.isDate {
				return v, nil
			}

			t, err := time.Parse(time.DateOnly, v)
			if err != nil {
				// Not a plain date (DATETIME, TIME): delivered as the text.
				return v, nil //nolint:nilerr // the text is the value
			}
			return t, nil
		}
	case avro.Int:
		switch v := value.(type) {
		case int:
			if v <= math.MaxInt32 && v >= math.MinInt32 {
				return int32(v), nil
			}
			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)

		case int8:
			return int32(v), nil
		case int16:
			return int32(v), nil
		case int64:
			if v >= math.MinInt32 && v <= math.MaxInt32 {
				return int32(v), nil
			}

			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)
		case uint8:
			return int32(v), nil
		case uint16:
			return int32(v), nil
		case uint32:
			if v <= math.MaxInt32 {
				return int32(v), nil
			}

			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)
		case uint64:
			if v <= math.MaxInt32 {
				return int32(v), nil
			}

			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)
		}

		return value, nil
	case avro.Long:
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case []uint8:
			// In a snapshot the driver returns a BIGINT UNSIGNED above
			// 2^63-1 as its decimal digits (it has no Go integer type for
			// it). This branch used to fold the bytes as a big-endian
			// integer, a leftover from when BIT mapped to long, and emitted a
			// valid but wrong number (18446744073709551615 became
			// 3474867090068222261). BIT maps to fixed. Digits of any
			// integer are accepted, so the column's signedness does not
			// matter here.
			if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				return i, nil
			}
			u, err := strconv.ParseUint(string(v), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("column %s.%s: unexpected value %q for an Avro long: %w", s.table, column, v, err)
			}
			return s.unsignedToLong(column, u)
		case uint32:
			return int64(v), nil
		case uint64:
			// CDC: canal returns every BIGINT UNSIGNED as uint64.
			return s.unsignedToLong(column, v)
		}

		return value, nil
	case avro.Double:
		switch v := value.(type) {
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				// Unchanged here: the encoder rejects the text, so the record
				// is not emitted. A stable error is part of #3040.
				return v, nil //nolint:nilerr // see above
			}

			return f, nil
		case []byte:
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return v, nil //nolint:nilerr // as for string above
			}

			return f, nil
		}
	case avro.Boolean:
		switch v := value.(type) {
		case int8:
			return v != 0, nil
		case uint8:
			return v != 0, nil
		}
	case avro.Fixed:
		switch v := value.(type) {
		case int64:
			// canal.Canal parses mysql bit column as an int64, so to be
			// consistent with snapshot mode we need to manually parse the int
			// into a slice of bytes.
			return int64ToBytes(v), nil
		case []byte:
			if t.isBit {
				// Because of our current limitation at
				// https://github.com/go-sql-driver/mysql/issues/1672
				// we map BIT(N) columns to avro fixed[8] data type, so we need to
				// do this.
				byte8Table := [8]byte{}

				// Should never happen, but just in case.
				if len(v) > 8 {
					v = v[len(v)-8:]
				}
				copy(byte8Table[8-len(v):], v)

				return byte8Table[:], nil
			}
		}
	case avro.Bytes:
		if v, ok := value.(string); ok {
			return []byte(v), nil
		}
	default:
		return value, nil
	}
	return value, nil
}

// unsignedToLong converts an unsigned value to an Avro long, or fails with
// CodeUnsignedOverflow.
//
// Invariant 6: a value the schema type cannot represent halts the source with
// CodeUnsignedOverflow. It is never wrapped, truncated or folded into another
// number, in a snapshot or in CDC.
func (s *schemaMapper) unsignedToLong(column string, v uint64) (any, error) {
	if v <= math.MaxInt64 {
		return int64(v), nil
	}
	return nil, s.types.unsignedOverflowError(s.table, column, v)
}

func defaultValueForType(t avro.Type) any {
	switch t {
	case avro.Array:
		return []any{}
	case avro.Map:
		return map[string]any{}
	case avro.String:
		return ""
	case avro.Bytes:
		return []byte{}
	case avro.Int:
		return int32(0)
	case avro.Long:
		return int64(0)
	case avro.Float:
		return float32(0)
	case avro.Double:
		return float64(0)
	case avro.Boolean:
		return false
	case avro.Null:
		return nil
	case avro.Record, avro.Error, avro.Ref, avro.Enum, avro.Fixed, avro.Union:
		return nil
	default:
		return nil
	}
}

// int64ToBytes transforms an int64 to a slice of bytes without leading zeros.
func int64ToBytes(i int64) []byte {
	bs := [8]byte{}

	//nolint:gosec // the overflow that can happen here in this case is fine.
	v := uint64(i)
	binary.BigEndian.PutUint64(bs[:], v)
	return bs[:]
}
