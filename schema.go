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

type User struct {
	Name string
}

type schemaManager struct {
	schema   *avro.RecordSchema
	colTypes map[string]avro.Type
}

func newSchemaManager() *schemaManager {
	// all fields can be nil, but we don't want consumers to have to worry about that
	return &schemaManager{}
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

func (s *schemaManager) create(ctx context.Context, table string, colTypes []*sql.ColumnType) error {
	if s.schema != nil {
		return nil
	}

	s.colTypes = make(map[string]avro.Type)
	var fields []*avro.Field
	for _, colType := range colTypes {
		field, err := colTypeToAvroField(colType)
		if err != nil {
			return err
		}

		fields = append(fields, field)

		s.colTypes[colType.Name()] = field.Type().Type()
	}

	recordSchema, err := avro.NewRecordSchema(table, "mysql", fields)
	if err != nil {
		return err
	}

	fmt.Println(recordSchema.Name())
	_, err = schema.Create(ctx, schema.TypeAvro, recordSchema.Name(), []byte(recordSchema.String()))
	if err != nil {
		return err
	}

	s.schema = recordSchema

	return nil
}

func (s *schemaManager) formatValue(column string, value any) any {
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
