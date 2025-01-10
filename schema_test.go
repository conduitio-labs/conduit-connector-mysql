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
	"encoding/json"
	"testing"
	"time"

	testutils "github.com/conduitio-labs/conduit-connector-mysql/test"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/google/go-cmp/cmp"
	"github.com/hamba/avro/v2"
	"github.com/matryer/is"
)

var tableName = "schema_examples"

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
	fields := []*avro.Field{
		// Numeric Types
		field(is, "tiny_int_col", avro.Int),
		field(is, "small_int_col", avro.Int),
		field(is, "medium_int_col", avro.Int),
		field(is, "int_col", avro.Int),
		field(is, "big_int_col", avro.Long),
		field(is, "decimal_col", avro.Double),
		field(is, "float_col", avro.Double),
		field(is, "double_col", avro.Double),
		field(is, "bit_col", avro.Bytes),

		// String Types
		field(is, "char_col", avro.String),
		field(is, "varchar_col", avro.String),
		field(is, "tiny_text_col", avro.String),
		field(is, "text_col", avro.String),
		field(is, "medium_text_col", avro.String),
		field(is, "long_text_col", avro.String),

		// Binary Types
		field(is, "binary_col", avro.Bytes),
		field(is, "varbinary_col", avro.Bytes),
		field(is, "tiny_blob_col", avro.Bytes),
		field(is, "blob_col", avro.Bytes),
		field(is, "medium_blob_col", avro.Bytes),
		field(is, "long_blob_col", avro.Bytes),

		// Date and Time Types
		field(is, "date_col", avro.String),
		field(is, "time_col", avro.String),
		field(is, "datetime_col", avro.String),
		field(is, "timestamp_col", avro.String),
		field(is, "year_col", avro.Long),

		// Other Types
		field(is, "enum_col", avro.String),
		field(is, "set_col", avro.String),
		field(is, "json_col", avro.String),
	}

	recordSchema, err := avro.NewRecordSchema(tableName+"_payload", "mysql", fields)
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

	type SchemaExample struct {
		// Numeric Types
		TinyIntCol   int8    `gorm:"column:tiny_int_col;type:tinyint"`
		SmallIntCol  int16   `gorm:"column:small_int_col;type:smallint"`
		MediumIntCol int32   `gorm:"column:medium_int_col;type:mediumint"`
		IntCol       int32   `gorm:"column:int_col;type:int"`
		BigIntCol    int64   `gorm:"column:big_int_col;type:bigint"`
		DecimalCol   float64 `gorm:"column:decimal_col;type:decimal(10,2)"`
		FloatCol     float32 `gorm:"column:float_col;type:float"`
		DoubleCol    float64 `gorm:"column:double_col;type:double"`
		BitCol       []uint8 `gorm:"column:bit_col;type:bit(1)"`

		// String Types
		CharCol       string `gorm:"column:char_col;type:char(10)"`
		VarcharCol    string `gorm:"column:varchar_col;type:varchar(255)"`
		TinyTextCol   string `gorm:"column:tiny_text_col;type:tinytext"`
		TextCol       string `gorm:"column:text_col;type:text"`
		MediumTextCol string `gorm:"column:medium_text_col;type:mediumtext"`
		LongTextCol   string `gorm:"column:long_text_col;type:longtext"`

		// Binary Types
		BinaryCol     []byte `gorm:"column:binary_col;type:binary(10)"`
		VarbinaryCol  []byte `gorm:"column:varbinary_col;type:varbinary(255)"`
		TinyBlobCol   []byte `gorm:"column:tiny_blob_col;type:tinyblob"`
		BlobCol       []byte `gorm:"column:blob_col;type:blob"`
		MediumBlobCol []byte `gorm:"column:medium_blob_col;type:mediumblob"`
		LongBlobCol   []byte `gorm:"column:long_blob_col;type:longblob"`

		// Date and Time Types
		DateCol      time.Time `gorm:"column:date_col;type:date"`
		TimeCol      time.Time `gorm:"column:time_col;type:time"`
		DateTimeCol  time.Time `gorm:"column:datetime_col;type:datetime"`
		TimestampCol time.Time `gorm:"column:timestamp_col;type:timestamp"`
		YearCol      int       `gorm:"column:year_col;type:year"`

		// Other Types
		EnumCol string `gorm:"column:enum_col;type:enum('value1','value2','value3')"`
		SetCol  string `gorm:"column:set_col;type:set('value1','value2','value3')"`
		JSONCol string `gorm:"column:json_col;type:json"`
	}

	is.NoErr(db.Migrator().DropTable(&SchemaExample{}))
	is.NoErr(db.AutoMigrate(&SchemaExample{}))

	testData := map[string]any{
		"tiny_int_col":    int8(127),
		"small_int_col":   int16(32767),
		"medium_int_col":  int32(8388607),
		"int_col":         2147483647,
		"big_int_col":     int64(9223372036854775807),
		"decimal_col":     123.45,
		"float_col":       float32(123.45),
		"double_col":      123.45,
		"bit_col":         []uint8{1},
		"char_col":        "char",
		"varchar_col":     "varchar",
		"tiny_text_col":   "tiny text",
		"text_col":        "text",
		"medium_text_col": "medium text",
		"long_text_col":   "long text",
		"binary_col":      []byte("binary"),
		"varbinary_col":   []byte("varbinary"),
		"tiny_blob_col":   []byte("tiny blob"),
		"blob_col":        []byte("blob"),
		"medium_blob_col": []byte("medium blob"),
		"long_blob_col":   []byte("long blob"),
		"date_col":        time.Now(),
		"time_col":        time.Now(),
		"datetime_col":    time.Now(),
		"timestamp_col":   time.Now(),
		"year_col":        2024,
		"enum_col":        "value1",
		"set_col":         "value1,value2",
		"json_col":        `{"key": "value"}`,
	}

	is.NoErr(db.Model(&SchemaExample{}).Create(&testData).Error)

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)

	colTypes, err := sqlxRowsToAvroCol(rows)
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

	s, err := schema.Get(ctx, tableName+"_payload", 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)

	expectedSchema := expectedPayloadRecordSchema(is)
	is.Equal("", cmp.Diff(expectedSchema, actualSchema)) // expected schema != actual schema
	is.Equal("", cmp.Diff(testData, formatted))          // expected data != actual data
}

func TestSchema_Key(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	type SchemaExample struct {
		ID int    `gorm:"column:id;type:int"`
		F1 string `gorm:"column:f1;type:varchar(255)"`
	}

	is.NoErr(db.Migrator().DropTable(&SchemaExample{}))
	is.NoErr(db.AutoMigrate(&SchemaExample{}))

	is.NoErr(db.Create(&SchemaExample{ID: 1, F1: "test"}).Error)

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)

	colTypes, err := sqlxRowsToAvroCol(rows)
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
