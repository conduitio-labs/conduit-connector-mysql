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
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/hamba/avro/v2"
	"github.com/jmoiron/sqlx"
)

// schemaMapper maps mysql schemas to conduit avro schemas, and it uses them to
// format values.
type schemaMapper struct {
	schema   *schemaSubjectVersion
	colTypes map[string]*avroNamedType
}

func newSchemaMapper() *schemaMapper {
	return &schemaMapper{
		colTypes: make(map[string]*avroNamedType),
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

		avroCol := &avroNamedType{avroType: avroType, Name: colType.Name()}
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
	if avroCol.isBit {
		// Current limitations in the mysql driver that we use don't allow use
		// to get the N from BIT(N) mysql columns. To track support for this
		// feature refer to https://github.com/go-sql-driver/mysql/issues/1672
		fixed8Size := 8

		fixed, err := avro.NewFixedSchema(avroCol.Name+"_fixed", "", fixed8Size, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create fixed schema for bit column %s: %w", avroCol.Name, err)
		}

		field, err := avro.NewField(avroCol.Name, fixed)
		if err != nil {
			return nil, fmt.Errorf("failed to create avro field for bit column %s: %w", avroCol.Name, err)
		}

		return field, nil
	}

	primitive := avro.NewPrimitiveSchema(avroCol.Type, nil)

	nameField, err := avro.NewField(avroCol.Name, primitive)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro field for column %s: %w", avroCol.Name, err)
	}

	return nameField, nil
}

type schemaSubjectVersion struct {
	subject string
	version int
}

func (s *schemaMapper) createPayloadSchema(
	ctx context.Context, schemaName string, colTypes []*avroNamedType,
) (*schemaSubjectVersion, error) {
	return s.createSchema(ctx, schemaName+"_payload", colTypes)
}

func (s *schemaMapper) createKeySchema(
	ctx context.Context, schemaName string, colTypes []*avroNamedType,
) (*schemaSubjectVersion, error) {
	return s.createSchema(ctx, schemaName+"_key", colTypes)
}

func (s *schemaMapper) createSchema(
	ctx context.Context, table string, mysqlCols []*avroNamedType,
) (*schemaSubjectVersion, error) {
	if s.schema != nil {
		return s.schema, nil
	}

	fields := make([]*avro.Field, 0, len(mysqlCols))
	for _, colType := range mysqlCols {
		field, err := colTypeToAvroField(colType)
		if err != nil {
			return nil, fmt.Errorf("failed to create payload schema: %w", err)
		}

		fields = append(fields, field)

		s.colTypes[colType.Name] = colType
	}

	recordSchema, err := avro.NewRecordSchema(table, "mysql", fields)
	if err != nil {
		return nil, fmt.Errorf("failed to create payload schema: %w", err)
	}

	schema, err := schema.Create(
		ctx, schema.TypeAvro, recordSchema.Name(), []byte(recordSchema.String()))
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
func (s *schemaMapper) formatValue(ctx context.Context, column string, value any) any {
	t, found := s.colTypes[column]
	if !found {
		// In snapshot mode, this should never happen.
		// In CDC mode, to prevent getting into here we make sure to instantiate the
		// schema mapper for each row event.
		sdk.Logger(ctx).Warn().Msgf("column \"%v\" not found", column)
		return value
	}

	// Each of the following branches handles different datatype parsing
	// behaviour between database/sql and go-mysql-org/go-mysql/canal
	// dependencies. We need this to make sure that a row emitted in snapshot mode
	// and updated in cdc mode have the same schema.

	// We manually convert nil values into the go zero value equivalent so that
	// we don't need to handle NULL complexity into the schema.
	// However, we might want to reflect nullability of the datatype in the future.
	if value == nil {
		return defaultValueForType(t.Type)
	}

	switch t.Type {
	case avro.String:
		switch v := value.(type) {
		case []uint8:
			return string(v)
		case string:
			if !t.isDate {
				return v
			}

			t, err := time.Parse(time.DateOnly, v)
			if err != nil {
				return v
			}
			return t
		}
	case avro.Int:
		switch v := value.(type) {
		case int:
			if v <= math.MaxInt32 && v >= math.MinInt32 {
				return int32(v)
			}
			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)

		case int8:
			return int32(v)
		case int16:
			return int32(v)
		case int64:
			if v >= math.MinInt32 && v <= math.MaxInt32 {
				return int32(v)
			}

			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)
		case uint8:
			return int32(v)
		case uint16:
			return int32(v)
		case uint32:
			if v <= math.MaxInt32 {
				return int32(v)
			}

			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)
		case uint64:
			if v <= math.MaxInt32 {
				return int32(v)
			}

			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)
		}

		return value
	case avro.Long:
		switch v := value.(type) {
		case int:
			return int64(v)
		case []uint8:
			// this handles the mysql bit datatype. When snapshotting will be
			// represented as slice of bytes, so we manually convert it to the
			// corresponding avro.Long datatype.

			if len(v) > 0 {
				var result int64
				for i := 0; i < len(v); i++ {
					result = result<<8 + int64(v[i])
				}
				return result
			}
			return int64(0)

		case uint32:
			return int64(v)
		case uint64:
			if v <= math.MaxInt64 {
				return int64(v)
			}

			sdk.Logger(ctx).Warn().Msgf("value %v for column %s cannot be encoded in avro int", v, t.Name)
		}

		return value
	case avro.Double:
		switch v := value.(type) {
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return v
			}

			return f
		case []byte:
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				return v
			}

			return f
		}
	case avro.Boolean:
		switch v := value.(type) {
		case int8:
			return v != 0
		case uint8:
			return v != 0
		}
	case avro.Fixed:
		switch v := value.(type) {
		case int64:
			// canal.Canal parses mysql bit column as an int64, so to be
			// consistent with snapshot mode we need to manually parse the int
			// into a slice of bytes.
			return int64ToBytes(v)
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

				return byte8Table[:]
			}
		}
	case avro.Bytes:
		if v, ok := value.(string); ok {
			return []byte(v)
		}
	default:
		return value
	}
	return value
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
