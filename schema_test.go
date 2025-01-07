package mysql

import (
	"context"
	"encoding/json"
	"testing"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/hamba/avro/v2"
	"github.com/matryer/is"
)

var tableName = "table_schema_tests"

func field(is *is.I, fieldName string, t avro.Type) *avro.Field {
	field, err := avro.NewField(fieldName, avro.NewPrimitiveSchema(t, nil))
	is.NoErr(err)

	return field
}

func toMap(is *is.I, bs []byte) map[string]any {
	m := map[string]any{}
	is.NoErr(json.Unmarshal(bs, &m))

	return m
}

func expectedPayloadRecordSchema(is *is.I) map[string]any {
	recordSchema, err := avro.NewRecordSchema(tableName+"_payload", "mysql", []*avro.Field{
		field(is, "f1", avro.String),
		field(is, "f2", avro.Long),
	})
	is.NoErr(err)

	bs, err := recordSchema.MarshalJSON()
	is.NoErr(err)

	return toMap(is, bs)
}

func expectedKeyRecordSchema(is *is.I) map[string]any {
	recordSchema, err := avro.NewRecordSchema(tableName+"_key", "mysql", []*avro.Field{
		field(is, "f1", avro.String),
	})
	is.NoErr(err)

	bs, err := recordSchema.MarshalJSON()
	is.NoErr(err)

	return toMap(is, bs)
}

func TestSchema_Payload(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type Table struct {
		F1 string `gorm:"column:f1;type:varchar(255)"`
		F2 int    `gorm:"column:f2;type:int"`
	}

	is.NoErr(db.Migrator().DropTable(&Table{}))
	is.NoErr(db.AutoMigrate(&Table{}))

	is.NoErr(db.Create(&Table{F1: "test", F2: 1}).Error)

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)

	colTypes, err := parseMultipleSqlColtypes(rows)
	is.NoErr(err)

	// Test payload schema
	payloadSchemaManager := newSchemaMapper()
	_, err = payloadSchemaManager.createPayloadSchema(ctx, tableName, colTypes)
	is.NoErr(err)

	row := db.SqlxDB.QueryRowx("select * from " + tableName)
	dest := map[string]any{}
	is.NoErr(row.MapScan(dest))

	formatted := map[string]any{}
	for k, v := range dest {
		formatted[k] = payloadSchemaManager.formatValue(k, v)
	}

	expected := map[string]any{
		"f1": "test",
		"f2": int64(1), // MySQL returns int64 for INT columns
	}

	is.Equal("", cmp.Diff(expected, formatted))

	s, err := schema.Get(ctx, tableName+"_payload", 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)
	expectedSchema := expectedPayloadRecordSchema(is)

	is.Equal("", cmp.Diff(expectedSchema, actualSchema))
}

func TestSchema_Key(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type Table struct {
		ID int    `gorm:"column:id;type:int"`
		F1 string `gorm:"column:f1;type:varchar(255)"`
	}

	is.NoErr(db.Migrator().DropTable(&Table{}))
	is.NoErr(db.AutoMigrate(&Table{}))

	is.NoErr(db.Create(&Table{ID: 1, F1: "test"}).Error)

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)

	colTypes, err := parseMultipleSqlColtypes(rows)
	is.NoErr(err)

	keySchemaManager := newSchemaMapper()

	var f1Col *avroColType
	for _, colType := range colTypes {
		if colType.Name == "f1" {
			f1Col = colType
			break
		}
	}

	_, err = keySchemaManager.createKeySchema(ctx, tableName, f1Col)
	is.NoErr(err)

	s, err := schema.Get(ctx, tableName+"_key", 1)
	is.NoErr(err)

	actualKeySchema := toMap(is, s.Bytes)
	expectedKeySchema := expectedKeyRecordSchema(is)

	is.Equal("", cmp.Diff(expectedKeySchema, actualKeySchema))
}
