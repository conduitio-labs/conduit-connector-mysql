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
	"fmt"
	"math"
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
	colTypes map[string]*avroColType
}

func newSchemaMapper() *schemaMapper {
	return &schemaMapper{
		colTypes: make(map[string]*avroColType),
	}
}

type avroDatedType struct {
	Type   avro.Type
	isDate bool
}

var sqlColtypeToAvroDatedTypeMap = map[string]avroDatedType{
	// Numeric types
	"TINYINT":   {Type: avro.Int},
	"SMALLINT":  {Type: avro.Int},
	"MEDIUMINT": {Type: avro.Int},
	"INT":       {Type: avro.Int},
	"BIGINT":    {Type: avro.Long},
	"DECIMAL":   {Type: avro.Double},
	"NUMERIC":   {Type: avro.Double},
	"FLOAT":     {Type: avro.Double},
	"DOUBLE":    {Type: avro.Double},
	"BIT":       {Type: avro.Bytes},

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

type avroColType struct {
	avroDatedType
	Name string
}

func sqlxRowsToAvroCol(rows *sqlx.Rows) ([]*avroColType, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column types: %w", err)
	}

	avroCols := make([]*avroColType, len(colTypes))
	for i, colType := range colTypes {
		avroDatedType, ok := sqlColtypeToAvroDatedTypeMap[colType.DatabaseTypeName()]
		if !ok {
			return nil, fmt.Errorf(
				"failed to retrieve column type %s for %s",
				colType.DatabaseTypeName(), colType.Name())
		}

		avroCols[i] = &avroColType{avroDatedType: avroDatedType, Name: colType.Name()}
	}

	return avroCols, nil
}

var mysqlschemaTypeToAvroDatedTypeMap = map[int]avroDatedType{
	mysqlschema.TYPE_NUMBER:     {Type: avro.Int},
	mysqlschema.TYPE_FLOAT:      {Type: avro.Double},
	mysqlschema.TYPE_DECIMAL:    {Type: avro.Double},
	mysqlschema.TYPE_ENUM:       {Type: avro.String},
	mysqlschema.TYPE_SET:        {Type: avro.String},
	mysqlschema.TYPE_DATETIME:   {Type: avro.String, isDate: true},
	mysqlschema.TYPE_TIMESTAMP:  {Type: avro.String, isDate: true},
	mysqlschema.TYPE_DATE:       {Type: avro.String, isDate: true},
	mysqlschema.TYPE_TIME:       {Type: avro.String, isDate: true},
	mysqlschema.TYPE_BIT:        {Type: avro.Bytes},
	mysqlschema.TYPE_JSON:       {Type: avro.String},
	mysqlschema.TYPE_BINARY:     {Type: avro.Bytes},
	mysqlschema.TYPE_POINT:      {Type: avro.String},
	mysqlschema.TYPE_STRING:     {Type: avro.String},
	mysqlschema.TYPE_MEDIUM_INT: {Type: avro.Int},
}

var rawTypeToAvroTypeMap = map[string]avro.Type{
	"bigint":     avro.Long,
	"tinyblob":   avro.Bytes,
	"blob":       avro.Bytes,
	"mediumblob": avro.Bytes,
	"longblob":   avro.Bytes,
}

func mysqlSchemaToAvroCol(tableCol mysqlschema.TableColumn) (*avroColType, error) {
	datedType, ok := mysqlschemaTypeToAvroDatedTypeMap[tableCol.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported column type %s for column %s", tableCol.RawType, tableCol.Name)
	}

	avroColType := &avroColType{
		avroDatedType: datedType,
		Name:          tableCol.Name,
	}

	rawType, ok := rawTypeToAvroTypeMap[tableCol.RawType]
	if ok {
		avroColType.Type = rawType
	}

	return avroColType, nil
}

func colTypeToAvroField(avroCol *avroColType) (*avro.Field, error) {
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
	ctx context.Context, table string, mysqlCols []*avroColType,
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

	recordSchema, err := avro.NewRecordSchema(table+"_payload", "mysql", fields)
	if err != nil {
		return nil, fmt.Errorf("failed to create payload schema: %w", err)
	}

	schema, err := schema.Create(ctx, schema.TypeAvro, recordSchema.Name(), []byte(recordSchema.String()))
	if err != nil {
		return nil, fmt.Errorf("failed to create payload schema: %w", err)
	}

	s.schema = &schemaSubjectVersion{
		subject: schema.Subject,
		version: schema.Version,
	}

	return s.schema, nil
}

func (s *schemaMapper) createKeySchema(
	ctx context.Context, table string, colType *avroColType,
) (*schemaSubjectVersion, error) {
	if s.schema != nil {
		return s.schema, nil
	}

	field, err := colTypeToAvroField(colType)
	if err != nil {
		return nil, fmt.Errorf("failed to create key schema: %w", err)
	}

	recordSchema, err := avro.NewRecordSchema(table+"_key", "mysql", []*avro.Field{field})
	if err != nil {
		return nil, fmt.Errorf("failed to create key schema: %w", err)
	}

	s.colTypes[colType.Name] = colType

	schema, err := schema.Create(ctx, schema.TypeAvro, recordSchema.Name(), []byte(recordSchema.String()))
	if err != nil {
		return nil, fmt.Errorf("failed to create key schema: %w", err)
	}

	s.schema = &schemaSubjectVersion{
		subject: schema.Subject,
		version: schema.Version,
	}

	return s.schema, nil
}

// formatValue uses the stored avro types to format the value. This way we can
// better control what the mysql driver returns. It should be called after creating
// the schema, otherwise it won't do anything.
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
			if v < math.MaxInt32 && v > math.MinInt32 {
				return int32(v)
			}
		case int8:
			return int32(v)
		case int16:
			return int32(v)
		case int64:
			if v > math.MaxInt32 || v < math.MinInt32 {
				return v
			}
			return int32(v)
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

		case uint64:
			if v <= math.MaxInt64 {
				return int64(v)
			}
			// This will make avro encoding fail as it doesn't support uint64
			return v
		}

		return value
	case avro.Float:
		switch v := value.(type) {
		case float32:
			return v
		case float64:
			return float32(v)
		}
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
	case avro.Bytes:
		switch v := value.(type) {
		case int64:
			// canal.Canal parses mysql bit column as an int64, so to be
			// consistent with snapshot mode we need to manually parse the int
			// into a slice of bytes.
			return int64ToMysqlBit(v)
		case string:
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

func int64ToMysqlBit(i int64) []byte {
	bytes := make([]byte, 8)
	bytes[0] = byte(i >> 56)
	bytes[1] = byte(i >> 48)
	bytes[2] = byte(i >> 40)
	bytes[3] = byte(i >> 32)
	bytes[4] = byte(i >> 24)
	bytes[5] = byte(i >> 16)
	bytes[6] = byte(i >> 8)
	bytes[7] = byte(i)

	// Find first non-zero byte to trim leading zeros
	start := 0
	for start < 7 && bytes[start] == 0 {
		start++
	}

	return bytes[start:]
}
