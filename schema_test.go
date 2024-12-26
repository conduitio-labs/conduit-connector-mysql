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

type TableSchemaTest struct {
	F1 string `gorm:"column:f1;type:varchar(255)"`
	F2 int    `gorm:"column:f2;type:int"`
}

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

func expectedRecordSchema(is *is.I) map[string]any {
	recordSchema, err := avro.NewRecordSchema(tableName, "mysql", []*avro.Field{
		field(is, "f1", avro.String),
		field(is, "f2", avro.Long),
	})
	is.NoErr(err)

	bs, err := recordSchema.MarshalJSON()
	is.NoErr(err)

	return toMap(is, bs)
}

func TestSchema(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	is.NoErr(db.Migrator().DropTable(&TableSchemaTest{}))
	is.NoErr(db.AutoMigrate(&TableSchemaTest{}))

	err := db.Create(&TableSchemaTest{
		F1: "test",
		F2: 1,
	}).Error
	is.NoErr(err)

	rows, err := db.SqlxDB.Query("select * from " + tableName)
	is.NoErr(err)

	colTypes, err := rows.ColumnTypes()
	is.NoErr(err)

	schemaManager := newSchemaMapper()
	_, err = schemaManager.createPayloadSchema(ctx, tableName, colTypes)
	is.NoErr(err)

	row := db.SqlxDB.QueryRowx("select * from " + tableName)
	dest := map[string]any{}
	is.NoErr(row.MapScan(dest))

	formatted := map[string]any{}
	for k, v := range dest {
		formatted[k] = schemaManager.formatValue(k, v)
	}

	expected := map[string]any{
		"f1": "test",
		"f2": int64(1), // MySQL returns int64 for INT columns
	}

	is.Equal("", cmp.Diff(expected, formatted))

	s, err := schema.Get(ctx, tableName, 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)
	expectedSchema := expectedRecordSchema(is)

	is.Equal("", cmp.Diff(expectedSchema, actualSchema))
}
