package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
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
	case "BIGINT":
		avroType = avro.Long
	case "INT":
		avroType = avro.Int
	case "DATETIME":
		avroType = avro.String
	case "VARCHAR", "TEXT":
		avroType = avro.String
	default:
		return nil, fmt.Errorf("unsupported column type %s", typename)
	}

	return &avroColType{avroType, colType.Name()}, nil
}

func parseMultipleSqlColtypes(rows *sqlx.Rows) ([]*avroColType, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	avroCols := make([]*avroColType, len(colTypes))
	for i, colType := range colTypes {
		avroCol, err := sqlColTypeToAvroCol(colType)
		if err != nil {
			return nil, err
		}

		avroCols[i] = avroCol
	}

	return avroCols, nil
}

func mysqlSchemaToAvroCol(tableCol mysqlschema.TableColumn) (*avroColType, error) {
	var avroType avro.Type
	switch tableCol.Type {
	case mysqlschema.TYPE_NUMBER:
		avroType = avro.Long
	case mysqlschema.TYPE_FLOAT:
		avroType = avro.Float
	case mysqlschema.TYPE_DATETIME:
		avroType = avro.String
	case mysqlschema.TYPE_STRING:
		avroType = avro.String
	default:
		return nil, fmt.Errorf("unsupported column type %s for column %s", tableCol.RawType, tableCol.Name)
	}

	return &avroColType{avroType, tableCol.Name}, nil
}

func colTypeToAvroField(avroCol *avroColType) (*avro.Field, error) {
	primitive := avro.NewPrimitiveSchema(avroCol.Type, nil)

	nameField, err := avro.NewField(avroCol.Name, primitive)
	if err != nil {
		return nil, fmt.Errorf("failed to create field for column %s: %w", avroCol.Name, err)
	}

	return nameField, nil
}

// subVerSchema represents the (sub)ject and the (ver)sion of a schema
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
	var fields []*avro.Field
	for _, colType := range mysqlCols {
		field, err := colTypeToAvroField(colType)
		if err != nil {
			return nil, err
		}

		fields = append(fields, field)

		s.colTypes[colType.Name] = field.Type().Type()
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
	ctx context.Context, table string, colType *avroColType,
) (*subVerSchema, error) {
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

	s.colTypes[colType.Name] = field.Type().Type()

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
		msg := fmt.Sprintf("column \"%v\" not found", column)
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
