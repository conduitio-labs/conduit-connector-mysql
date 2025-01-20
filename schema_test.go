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

func field(is *is.I, fieldName string, t avro.Type) *avro.Field {
	field, err := avro.NewField(fieldName, avro.NewPrimitiveSchema(t, nil))
	is.NoErr(err)

	return field
}

func fixed8ByteField(is *is.I, fieldName string) *avro.Field {
	fixed, err := avro.NewFixedSchema(fieldName+"_fixed", "", 8, nil)
	is.NoErr(err)

	field, err := avro.NewField(fieldName, fixed)
	is.NoErr(err)

	return field
}

func toMap(is *is.I, bs []byte) map[string]any {
	m := map[string]any{}
	is.NoErr(json.Unmarshal(bs, &m))

	return m
}

func expectedKeyRecordSchema(is *is.I, tableName string) map[string]any {
	recordSchema, err := avro.NewRecordSchema(tableName+"_key", "mysql", []*avro.Field{
		field(is, "f1", avro.String),
	})
	is.NoErr(err)

	bs, err := recordSchema.MarshalJSON()
	is.NoErr(err)

	return toMap(is, bs)
}

type SchemaAllTypes struct {
	// Note: the go types specified here for each column aren't really relevant for
	// the schema tests, we just need the gorm struct tags. We use the avro
	// equivalent in go though to be consistent.

	// Signed Numeric Types
	TinyIntCol   int32 `gorm:"column:tiny_int_col;type:tinyint"`
	SmallIntCol  int32 `gorm:"column:small_int_col;type:smallint"`
	MediumIntCol int32 `gorm:"column:medium_int_col;type:mediumint"`
	IntCol       int32 `gorm:"column:int_col;type:int"`
	BigIntCol    int64 `gorm:"column:big_int_col;type:bigint"`

	// Unsigned Numeric Types
	UTinyIntCol   int32 `gorm:"column:utiny_int_col;type:tinyint unsigned"`
	USmallIntCol  int32 `gorm:"column:usmall_int_col;type:smallint unsigned"`
	UMediumIntCol int32 `gorm:"column:umedium_int_col;type:mediumint unsigned"`
	UIntCol       int32 `gorm:"column:uint_col;type:int unsigned"`
	UBigIntCol    int64 `gorm:"column:ubig_int_col;type:bigint unsigned"`

	DecimalCol float64 `gorm:"column:decimal_col;type:decimal(10,2)"`
	FloatCol   float32 `gorm:"column:float_col;type:float(24)"`
	DoubleCol  float64 `gorm:"column:double_col;type:double"`

	// Bit Types (most common)
	Bit1Col  []byte `gorm:"column:bit1_col;type:bit(1)"`
	Bit8Col  []byte `gorm:"column:bit8_col;type:bit(8)"`
	Bit64Col []byte `gorm:"column:bit64_col;type:bit(64)"`

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

	// enum and set datatypes are omitted, as they are difficult to test on CDC mode.
	// In the future we might want to also test those cases.
	JSONCol string `gorm:"column:json_col;type:json"`
}

func expectedPayloadRecordSchema(is *is.I, tableName string) map[string]any {
	fields := []*avro.Field{
		// Signed Numeric Types
		field(is, "tiny_int_col", avro.Int),
		field(is, "small_int_col", avro.Int),
		field(is, "medium_int_col", avro.Int),
		field(is, "int_col", avro.Int),
		field(is, "big_int_col", avro.Long),

		// Unsigned Numeric Types
		field(is, "utiny_int_col", avro.Int),
		field(is, "usmall_int_col", avro.Int),
		field(is, "umedium_int_col", avro.Int),
		field(is, "uint_col", avro.Long),
		field(is, "ubig_int_col", avro.Long),

		field(is, "decimal_col", avro.Double),
		field(is, "float_col", avro.Float),
		field(is, "double_col", avro.Double),

		fixed8ByteField(is, "bit1_col"),
		fixed8ByteField(is, "bit8_col"),
		fixed8ByteField(is, "bit64_col"),

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
		field(is, "year_col", avro.Int),

		// Other Types
		field(is, "json_col", avro.String),
	}

	recordSchema, err := avro.NewRecordSchema(tableName+"_payload", "mysql", fields)
	is.NoErr(err)

	bs, err := recordSchema.MarshalJSON()
	is.NoErr(err)

	return toMap(is, bs)
}

func allTypesSnapshotTestData() map[string]any {
	// The column test values are written in the avro equivalent type, and not in
	// the mysql equivalent type, so that we can properly assert that formatting
	// works as expected.

	testData := map[string]any{
		// Signed max values
		"tiny_int_col":   int32(127),
		"small_int_col":  int32(32767),
		"medium_int_col": int32(8388607),
		"int_col":        int32(2147483647),
		"big_int_col":    int64(9223372036854775807),

		// Unsigned max values
		"utiny_int_col":   int32(255),
		"usmall_int_col":  int32(65535),
		"umedium_int_col": int32(16777215),
		"uint_col":        int64(429496729),
		"ubig_int_col":    int64(1844674407370955161),

		"float_col":       float32(123.5),
		"decimal_col":     123.5,
		"double_col":      123.5,
		"char_col":        "char",
		"varchar_col":     "varchar",
		"tiny_text_col":   "tiny text",
		"text_col":        "text",
		"medium_text_col": "medium text",
		"long_text_col":   "long text",

		// should be 10 bytes, as specified in the gorm struct
		"binary_col":      []byte("binary    "),
		"varbinary_col":   []byte("varbinary"),
		"tiny_blob_col":   []byte("tiny blob"),
		"blob_col":        []byte("blob"),
		"medium_blob_col": []byte("medium blob"),
		"long_blob_col":   []byte("long blob"),

		// MySQL date/time types have different precision levels - truncate to match what MySQL stores
		"date_col":      time.Now().UTC().Truncate(24 * time.Hour),
		"time_col":      time.Now().UTC().Truncate(time.Second),
		"datetime_col":  time.Now().UTC().Truncate(time.Second),
		"timestamp_col": time.Now().UTC().Truncate(time.Second),

		"year_col": int32(2025),
		"json_col": `{"key": "value"}`,
	}

	testData["bit1_col"] = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x01}
	testData["bit8_col"] = []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xAB}
	testData["bit64_col"] = []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0}

	return testData
}

func allTypesCDCTestData() map[string]any {
	data := allTypesSnapshotTestData()

	// for some reason canal.Canal treats json in a different manner than sqlx,
	// so we need to remove the space in between key an value.
	data["json_col"] = `{"key":"value"}`

	return data
}

func TestSchema_Payload_SQLX_Rows(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	is.NoErr(db.Migrator().DropTable(&SchemaAllTypes{}))
	is.NoErr(db.AutoMigrate(&SchemaAllTypes{}))

	testData := allTypesSnapshotTestData()

	is.NoErr(db.Model(&SchemaAllTypes{}).Create(&testData).Error)
	tableName := testutils.TableName(is, db, &SchemaAllTypes{})

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
		formatted[k] = payloadSchemaManager.formatValue(ctx, k, v)
	}

	s, err := schema.Get(ctx, tableName+"_payload", 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)
	expectedSchema := expectedPayloadRecordSchema(is, tableName)

	is.Equal("", cmp.Diff(expectedSchema, actualSchema)) // expected schema != actual schema
	is.Equal("", cmp.Diff(testData, formatted))          // expected data != actual data
}

func TestSchema_Payload_canal_RowsEvent(t *testing.T) {
	is := is.New(t)
	db := testutils.NewDB(t)
	ctx := context.Background()

	is.NoErr(db.Migrator().DropTable(&SchemaAllTypes{}))
	is.NoErr(db.AutoMigrate(&SchemaAllTypes{}))

	testData := allTypesCDCTestData()

	tableName := testutils.TableName(is, db, &SchemaAllTypes{})

	rowsEvent := testutils.TriggerRowInsertEvent(ctx, is, tableName, func() {
		is.NoErr(db.Model(&SchemaAllTypes{}).Create(&testData).Error)
	})

	avroCols := make([]*avroNamedType, len(rowsEvent.Table.Columns))
	for i, col := range rowsEvent.Table.Columns {
		avroCol, err := mysqlSchemaToAvroCol(col)
		is.NoErr(err)
		avroCols[i] = avroCol
	}

	payloadSchemaManager := newSchemaMapper()
	_, err := payloadSchemaManager.createPayloadSchema(ctx, tableName, avroCols)
	is.NoErr(err)

	formatted := map[string]any{}
	for i, col := range rowsEvent.Table.Columns {
		formatted[col.Name] = payloadSchemaManager.formatValue(ctx, col.Name, rowsEvent.Rows[0][i])
	}

	s, err := schema.Get(ctx, tableName+"_payload", 1)
	is.NoErr(err)

	actualSchema := toMap(is, s.Bytes)
	expectedSchema := expectedPayloadRecordSchema(is, tableName)

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
	tableName := testutils.TableName(is, db, &SchemaExample{})

	rows, err := db.SqlxDB.Queryx("select * from " + tableName)
	is.NoErr(err)

	colTypes, err := sqlxRowsToAvroCol(rows)
	is.NoErr(err)

	keySchemaManager := newSchemaMapper()

	var f1Col *avroNamedType
	for _, colType := range colTypes {
		if colType.Name == "f1" {
			f1Col = colType
			break
		}
	}

	_, err = keySchemaManager.createKeySchema(ctx, tableName, []*avroColType{f1Col})
	is.NoErr(err)

	s, err := schema.Get(ctx, tableName+"_key", 1)
	is.NoErr(err)

	actualKeySchema := toMap(is, s.Bytes)
	expectedKeySchema := expectedKeyRecordSchema(is, tableName)

	is.Equal("", cmp.Diff(expectedKeySchema, actualKeySchema))
}
