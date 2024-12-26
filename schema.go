package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/hamba/avro/v2"
)

// schemaMapper creates conduit avro schemas from sql.ColumnTypes and formats values
// based on those.
type schemaMapper struct {
	schema   *subVerSchema
	colTypes map[string]avro.Type
}

func newSchemaMapper() *schemaMapper {
	// all fields can be nil, but we don't want consumers to have to worry about that
	return &schemaMapper{}
}

func colTypeToAvroField(colType *sql.ColumnType) (*avro.Field, error) {
	var avroType avro.Type
	switch colType.DatabaseTypeName() {
	case "BIGINT":
		avroType = avro.Long
	case "INT":
		avroType = avro.Int
	case "DATETIME":
		avroType = avro.String
	case "VARCHAR", "TEXT":
		avroType = avro.String
	default:
		return nil, fmt.Errorf("unsupported column type %s", colType.DatabaseTypeName())
	}

	primitive := avro.NewPrimitiveSchema(avroType, nil)


	nameField, err := avro.NewField(colType.Name(), primitive)
	if err != nil {
		return nil, fmt.Errorf("failed to create field for column %s: %w", colType.Name(), err)
	}

	return nameField, nil
}

// subVerSchema represents the (sub)ject and the (ver)sion of a schema
type subVerSchema struct {
	subject string
	version int
}

func (s *schemaMapper) createPayloadSchema(
	ctx context.Context, table string, colTypes []*sql.ColumnType) (*subVerSchema, error) {
	if s.schema != nil {
		return s.schema, nil
	}

	s.colTypes = make(map[string]avro.Type)
	var fields []*avro.Field
	for _, colType := range colTypes {
		field, err := colTypeToAvroField(colType)
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)

		s.colTypes[colType.Name()] = field.Type().Type()
	}

	recordSchema, err := avro.NewRecordSchema(table+"_payload", "mysql", fields)
	if err != nil {
		return nil, err
	}

	schema, err := schema.Create(ctx, schema.TypeAvro, recordSchema.Name(), []byte(recordSchema.String()))
	if err != nil {
		return nil, err
	}

	s.schema = &subVerSchema{
		subject: schema.Subject,
		version: schema.Version,
	}

	return s.schema, nil
}

func (s *schemaMapper) createKeySchema(
	ctx context.Context, table string, colType *sql.ColumnType) (*subVerSchema, error) {
	if s.schema != nil {
		return s.schema, nil
	}

	field, err := colTypeToAvroField(colType)
	if err != nil {
		return nil, err
	}

	recordSchema, err := avro.NewRecordSchema(table+"_key", "mysql", []*avro.Field{field})
	if err != nil {
		return nil, err
	}

	schema, err := schema.Create(ctx, schema.TypeAvro, recordSchema.Name(), []byte(recordSchema.String()))
	if err != nil {
		return nil, err
	}

	s.schema = &subVerSchema{
		subject: schema.Subject,
		version: schema.Version,
	}

	return s.schema, nil
}

func (s *schemaMapper) formatValue(column string, value any) any {
	t, found := s.colTypes[column]
	if !found {
		msg := fmt.Sprint("column", column, "not found")
		panic(msg)
	}

	switch t {
	case avro.String:
		switch v := value.(type) {
		case time.Time:
			return v.UTC()
		case []uint8:
			return string(v)
		}

		return value
	case avro.Int, avro.Long:
		switch v := value.(type) {
		case uint64:
			if v <= math.MaxInt64 {
				return int64(v)
			}
			// this will make avro encoding fail, as it doesn't support uint64.
			return v
		}

		return value
	default:
		return value
	}
}

/*

Numeric Data Types
TINYINT
SMALLINT
MEDIUMINT
INT or INTEGER
BIGINT
FLOAT
DOUBLE or DOUBLE PRECISION
DECIMAL or NUMERIC
BIT
BOOL or BOOLEAN
Date and Time Data Types
DATE
DATETIME
TIMESTAMP
TIME
YEAR
String Data Types
CHAR
VARCHAR
BINARY
VARBINARY
BLOB
TINYBLOB
BLOB
MEDIUMBLOB
LONGBLOB
TEXT
TINYTEXT
TEXT
MEDIUMTEXT
LONGTEXT
ENUM
SET
Spatial Data Types
GEOMETRY
POINT
LINESTRING
POLYGON
MULTIPOINT
MULTILINESTRING
MULTIPOLYGON
GEOMETRYCOLLECTION
JSON Data Type
JSON

*/
