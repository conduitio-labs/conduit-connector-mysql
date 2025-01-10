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

	"github.com/conduitio/conduit-connector-sdk/schema"
	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/hamba/avro/v2"
	"github.com/jmoiron/sqlx"
)

// schemaMapper creates conduit avro schemas from sql.ColumnTypes and formats values
// based on those.
type schemaMapper struct {
	schema   *subVerSchema
	colTypes map[string]avro.Type
}

func newSchemaMapper() *schemaMapper {
	return &schemaMapper{
		colTypes: make(map[string]avro.Type),
	}
}

// avroColType represents the avro type of a mysql column.
type avroColType struct {
	Type avro.Type
	Name string
}

func sqlColTypeToAvroCol(colType *sql.ColumnType) (*avroColType, error) {
	var avroType avro.Type
	switch typename := colType.DatabaseTypeName(); typename {
	// Numeric Types
	case "TINYINT":
		avroType = avro.Int
	case "SMALLINT":
		avroType = avro.Int
	case "MEDIUMINT":
		avroType = avro.Int
	case "INT":
		avroType = avro.Int
	case "BIGINT":
		avroType = avro.Long
	case "DECIMAL", "NUMERIC":
		avroType = avro.Double
	case "FLOAT":
		avroType = avro.Double
	case "DOUBLE":
		avroType = avro.Double
	case "BIT":
		avroType = avro.Bytes

	// String Types
	case "CHAR", "VARCHAR":
		avroType = avro.String
	case "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT":
		avroType = avro.String

	// Binary Types
	case "BINARY", "VARBINARY":
		avroType = avro.Bytes
	case "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		avroType = avro.Bytes

	// Date and Time Types
	case "DATE", "TIME", "DATETIME", "TIMESTAMP":
		avroType = avro.String
	case "YEAR":
		avroType = avro.Long

	// Other Types
	case "ENUM", "SET":
		avroType = avro.String
	case "JSON":
		avroType = avro.String

	default:
		return nil, fmt.Errorf("unsupported column type %s", typename)
	}

	return &avroColType{avroType, colType.Name()}, nil
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
	var avroType avro.Type
	switch tableCol.Type {
	case mysqlschema.TYPE_NUMBER:
		switch tableCol.RawType {
		case "tinyint", "smallint", "mediumint", "int":
			avroType = avro.Int
		case "bigint", "year":
			avroType = avro.Long
		case "decimal", "numeric", "float", "double":
			avroType = avro.Double
		case "bit":
			avroType = avro.Bytes
		default:
			avroType = avro.Int
		}
	case mysqlschema.TYPE_MEDIUM_INT:
		avroType = avro.Int
	case mysqlschema.TYPE_FLOAT:
		avroType = avro.Double
	case mysqlschema.TYPE_DECIMAL:
		avroType = avro.Double
	case mysqlschema.TYPE_ENUM:
		avroType = avro.String
	case mysqlschema.TYPE_SET:
		avroType = avro.String
	case mysqlschema.TYPE_DATETIME:
		avroType = avro.String
	case mysqlschema.TYPE_TIMESTAMP:
		avroType = avro.String
	case mysqlschema.TYPE_DATE:
		avroType = avro.String
	case mysqlschema.TYPE_TIME:
		avroType = avro.String
	case mysqlschema.TYPE_BIT:
		avroType = avro.Bytes
	case mysqlschema.TYPE_JSON:
		avroType = avro.String
	case mysqlschema.TYPE_BINARY:
		avroType = avro.Bytes
	case mysqlschema.TYPE_POINT:
		avroType = avro.String
	case mysqlschema.TYPE_STRING:
		switch tableCol.RawType {
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
			avroType = avro.Bytes
		default:
			avroType = avro.String
		}
	default:
		return nil, fmt.Errorf("unsupported column type %s for column %s", tableCol.RawType, tableCol.Name)
	}

	return &avroColType{avroType, tableCol.Name}, nil
}

func colTypeToAvroField(avroCol *avroColType) (*avro.Field, error) {
	primitive := avro.NewPrimitiveSchema(avroCol.Type, nil)

	nameField, err := avro.NewField(avroCol.Name, primitive)
	if err != nil {
		return nil, fmt.Errorf("failed to create avro field for column %s: %w", avroCol.Name, err)
	}

	return nameField, nil
}

// subVerSchema represents the (sub)ject and the (ver)sion of a schema.
type subVerSchema struct {
	subject string
	version int
}

func (s *schemaMapper) createPayloadSchema(
	ctx context.Context, table string, mysqlCols []*avroColType,
) (*subVerSchema, error) {
	if s.schema != nil {
		return s.schema, nil
	}

	s.colTypes = make(map[string]avro.Type)
	fields := make([]*avro.Field, 0, len(mysqlCols))
	for _, colType := range mysqlCols {
		field, err := colTypeToAvroField(colType)
		if err != nil {
			return nil, fmt.Errorf("failed to create payload schema: %w", err)
		}

		fields = append(fields, field)

		s.colTypes[colType.Name] = field.Type().Type()
	}

	recordSchema, err := avro.NewRecordSchema(table+"_payload", "mysql", fields)
	if err != nil {
		return nil, fmt.Errorf("failed to create payload schema: %w", err)
	}

	schema, err := schema.Create(ctx, schema.TypeAvro, recordSchema.Name(), []byte(recordSchema.String()))
	if err != nil {
		return nil, fmt.Errorf("failed to create payload schema: %w", err)
	}

	s.schema = &subVerSchema{
		subject: schema.Subject,
		version: schema.Version,
	}

	return s.schema, nil
}

func (s *schemaMapper) createKeySchema(
	ctx context.Context, table string, colType *avroColType,
) (*subVerSchema, error) {
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

	s.colTypes[colType.Name] = field.Type().Type()

	schema, err := schema.Create(ctx, schema.TypeAvro, recordSchema.Name(), []byte(recordSchema.String()))
	if err != nil {
		return nil, fmt.Errorf("failed to create key schema: %w", err)
	}

	s.schema = &subVerSchema{
		subject: schema.Subject,
		version: schema.Version,
	}

	return s.schema, nil
}

// formatValue uses the stored avro types to format the value. This way we can
// better control what the mysql driver returns.
func (s *schemaMapper) formatValue(column string, value any) any {
	t, found := s.colTypes[column]
	if !found {
		// In snapshot mode, this should never happen.
		// In CDC mode, to prevent getting into here we make sure to instantiate the
		// schema mapper for each row event.
		msg := fmt.Sprintf("column \"%v\" not found", column)
		panic(msg)
	}

	// Handle nil values
	if value == nil {
		return nil
	}

	switch t {
	case avro.String:
		switch v := value.(type) {
		case time.Time:
			return v.UTC()
		case []uint8:
			return string(v)
		case string:
			t, err := time.Parse(time.DateOnly, v)
			if err != nil {
				return v
			}
			return t.UTC()
		// Handle enum and set values from canal events
		case int64:
			return strconv.FormatInt(v, 10)
		// Handle date values from canal events
		case int32:
			// Convert MySQL internal date representation to string
			t := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).
				AddDate(0, 0, int(v))
			return t.Format("2006-01-02")
		}
	case avro.Int:
		switch v := value.(type) {
		case int8:
			return int32(v)
		case uint8:
			return int32(v)
		case int16:
			return int32(v)
		case uint16:
			return int32(v)
		case int32:
			return v
		case uint32:
			if v <= math.MaxInt32 {
				return int32(v)
			}
			return v
		case int:
			if v > math.MaxInt32 || v < math.MinInt32 {
				return v
			}
			return int32(v)
		case int64:
			if v > math.MaxInt32 || v < math.MinInt32 {
				return v
			}
			return int32(v)
		case uint64:
			if v > math.MaxInt32 {
				return v
			}
			return int32(v)
		}
	case avro.Long:
		switch v := value.(type) {
		case int8:
			return int64(v)
		case uint8:
			return int64(v)
		case int16:
			return int64(v)
		case uint16:
			return int64(v)
		case int32:
			return int64(v)
		case uint32:
			return int64(v)
		case int:
			return int64(v)
		case int64:
			return v
		case uint64:
			if v <= math.MaxInt64 {
				return int64(v)
			}
			// This will make avro encoding fail as it doesn't support uint64
			return v
		}
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
			f, err := strconv.ParseFloat(string(v), 64)
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
		case float32:
			return float64(v)
		case float64:
			return v
		}
	case avro.Boolean:
		switch v := value.(type) {
		case bool:
			return v
		case int8:
			return v != 0
		case uint8:
			return v != 0
		}
	case avro.Bytes:
		switch v := value.(type) {
		case int64:
			return []byte{uint8(v)}
		case []byte:
			return v
		case string:
			return []byte(v)
		}
	default:
		return value
	}
	return value
}
