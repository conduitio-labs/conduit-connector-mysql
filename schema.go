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
	"database/sql"
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

type avroColType struct {
	isDate bool
	Type   avro.Type
	Name   string
}

func sqlColTypeToAvroCol(colType *sql.ColumnType) (*avroColType, error) {
	avroColType := &avroColType{
		Name: colType.Name(),
	}
	switch typename := colType.DatabaseTypeName(); typename {
	// Numeric Types
	case "TINYINT":
		avroColType.Type = avro.Int
	case "SMALLINT":
		avroColType.Type = avro.Int
	case "MEDIUMINT":
		avroColType.Type = avro.Int
	case "INT":
		avroColType.Type = avro.Int
	case "BIGINT":
		avroColType.Type = avro.Long
	case "DECIMAL", "NUMERIC":
		avroColType.Type = avro.Double
	case "FLOAT":
		avroColType.Type = avro.Double
	case "DOUBLE":
		avroColType.Type = avro.Double
	case "BIT":
		avroColType.Type = avro.Bytes

	// String Types
	case "CHAR", "VARCHAR":
		avroColType.Type = avro.String
	case "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
		avroColType.Type = avro.String

	// Binary Types
	case "BINARY", "VARBINARY":
		avroColType.Type = avro.Bytes
	case "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		avroColType.Type = avro.Bytes

	// Date and Time Types
	case "DATE", "TIME", "DATETIME", "TIMESTAMP":
		avroColType.isDate = true
		avroColType.Type = avro.String
	case "YEAR":
		avroColType.isDate = true
		avroColType.Type = avro.Long

	// Other Types
	case "ENUM", "SET":
		avroColType.Type = avro.String
	case "JSON":
		avroColType.Type = avro.String

	default:
		return nil, fmt.Errorf("unsupported column type %q for column %q", typename, colType.Name())
	}

	return avroColType, nil
}

func sqlxRowsToAvroCol(rows *sqlx.Rows) ([]*avroColType, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column types: %w", err)
	}

	avroCols := make([]*avroColType, len(colTypes))
	for i, colType := range colTypes {
		avroCol, err := sqlColTypeToAvroCol(colType)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve column types: %w", err)
		}

		avroCols[i] = avroCol
	}

	return avroCols, nil
}

func mysqlSchemaToAvroCol(tableCol mysqlschema.TableColumn) (*avroColType, error) {
	avroColType := &avroColType{Name: tableCol.Name}
	switch tableCol.Type {
	case mysqlschema.TYPE_NUMBER:
		switch tableCol.RawType {
		case "tinyint", "smallint", "mediumint", "int":
			avroColType.Type = avro.Int
		case "bigint", "year":
			avroColType.Type = avro.Long
		case "decimal", "numeric", "float", "double":
			avroColType.Type = avro.Double
		case "bit":
			avroColType.Type = avro.Bytes
		default:
			avroColType.Type = avro.Int
		}
	case mysqlschema.TYPE_MEDIUM_INT:
		avroColType.Type = avro.Int
	case mysqlschema.TYPE_FLOAT:
		avroColType.Type = avro.Double
	case mysqlschema.TYPE_DECIMAL:
		avroColType.Type = avro.Double
	case mysqlschema.TYPE_ENUM:
		avroColType.Type = avro.String
	case mysqlschema.TYPE_SET:
		avroColType.Type = avro.String
	case mysqlschema.TYPE_DATETIME:
		avroColType.isDate = true
		avroColType.Type = avro.String
	case mysqlschema.TYPE_TIMESTAMP:
		avroColType.isDate = true
		avroColType.Type = avro.String
	case mysqlschema.TYPE_DATE:
		avroColType.isDate = true
		avroColType.Type = avro.String
	case mysqlschema.TYPE_TIME:
		avroColType.isDate = true
		avroColType.Type = avro.String
	case mysqlschema.TYPE_BIT:
		avroColType.Type = avro.Bytes
	case mysqlschema.TYPE_JSON:
		avroColType.Type = avro.String
	case mysqlschema.TYPE_BINARY:
		avroColType.Type = avro.Bytes
	case mysqlschema.TYPE_POINT:
		avroColType.Type = avro.String
	case mysqlschema.TYPE_STRING:
		switch tableCol.RawType {
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
			avroColType.Type = avro.Bytes
		default:
			avroColType.Type = avro.String
		}
	default:
		return nil, fmt.Errorf("unsupported column type %s for column %s", tableCol.RawType, tableCol.Name)
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
