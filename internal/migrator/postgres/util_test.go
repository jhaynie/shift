package postgres

import (
	"testing"

	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/jhaynie/shift/internal/util"
	"github.com/stretchr/testify/assert"
)

func assertColumnDetail(t *testing.T, expectType string, expectArray bool, detail types.ColumnDetail) {
	val, isArray := toUDTName(detail)
	assert.Equal(t, expectType, val)
	assert.Equal(t, expectArray, isArray)
}

func TestToUDTName(t *testing.T) {
	assertColumnDetail(t, "", false, types.ColumnDetail{})
	assertColumnDetail(t, "text", false, types.ColumnDetail{UDTName: "text"})
	assertColumnDetail(t, "text[]", true, types.ColumnDetail{UDTName: "_text"})
	assertColumnDetail(t, "varchar(255)", false, types.ColumnDetail{UDTName: "varchar", MaxLength: util.Ptr(int64(255))})
	assertColumnDetail(t, "int8", false, types.ColumnDetail{DataType: "int", UDTName: "int8", NumericPrecision: util.Ptr(int64(64))})
	assertColumnDetail(t, "int4", false, types.ColumnDetail{DataType: "int", UDTName: "int4", NumericPrecision: util.Ptr(int64(32))})
	assertColumnDetail(t, "int4", false, types.ColumnDetail{DataType: "int", UDTName: "int4", NumericPrecision: util.Ptr(int64(32))})
	assertColumnDetail(t, "float4", false, types.ColumnDetail{DataType: "float", UDTName: "float4", NumericPrecision: util.Ptr(int64(24))})
	assertColumnDetail(t, "float8", false, types.ColumnDetail{DataType: "float", UDTName: "float8", NumericPrecision: util.Ptr(int64(64))})
	assertColumnDetail(t, "double precision", false, types.ColumnDetail{DataType: "float", UDTName: "float8", NumericPrecision: util.Ptr(int64(53))})
	assertColumnDetail(t, "numeric(10)", false, types.ColumnDetail{DataType: "int", UDTName: "numeric", NumericPrecision: util.Ptr(int64(10))})
	assertColumnDetail(t, "numeric(10,3)", false, types.ColumnDetail{DataType: "int", UDTName: "numeric", NumericPrecision: util.Ptr(int64(10)), NumericScale: util.Ptr(int64(3))})
	assertColumnDetail(t, "int2", false, types.ColumnDetail{DataType: "int", UDTName: "int2", NumericPrecision: util.Ptr(int64(16))})
}

func TestToNativeType(t *testing.T) {
	assert.Equal(t, "uuid", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{NativeType: &schema.SchemaJsonTablesElemColumnsElemNativeType{Postgres: util.Ptr("uuid")}}).Postgres)
	assert.Equal(t, "boolean", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeBoolean}).Postgres)
	assert.Equal(t, "timestamp with time zone", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeDatetime}).Postgres)
	assert.Equal(t, "double precision", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeFloat}).Postgres)
	assert.Equal(t, "real", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeFloat, MaxLength: util.Ptr(int(32))}).Postgres)
	assert.Equal(t, "numeric(10)", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, MaxLength: util.Ptr(int(10))}).Postgres)
	assert.Equal(t, "numeric(10,2)", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 10, Scale: util.Ptr(float64(2))}}).Postgres)
	assert.Equal(t, "numeric(10)", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 10}}).Postgres)
	assert.Equal(t, "smallint", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 16}}).Postgres)
	assert.Equal(t, "int4", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 32}}).Postgres)
	assert.Equal(t, "int8", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 64}}).Postgres)
	assert.Equal(t, "int8", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeInt}).Postgres)
	assert.Equal(t, "text", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeString}).Postgres)
	assert.Equal(t, "varchar(255)", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeString, MaxLength: util.Ptr(int(255))}).Postgres)
	assert.Equal(t, "bytea", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeBinary)}).Postgres)
	assert.Equal(t, "bit", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeBit)}).Postgres)
	assert.Equal(t, "bit(2)", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeBit), MaxLength: util.Ptr(2)}).Postgres)
	assert.Equal(t, "jsonb", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeJson)}).Postgres)
	assert.Equal(t, "uuid", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeUuid)}).Postgres)
}

func TestToNativeTypeArray(t *testing.T) {
	assert.Equal(t, "boolean[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeBoolean}).Postgres)
	assert.Equal(t, "timestamp with time zone[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeDatetime}).Postgres)
	assert.Equal(t, "double precision[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeFloat}).Postgres)
	assert.Equal(t, "real[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeFloat, MaxLength: util.Ptr(int(32))}).Postgres)
	assert.Equal(t, "numeric(10)[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, MaxLength: util.Ptr(int(10))}).Postgres)
	assert.Equal(t, "numeric(10,2)[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 10, Scale: util.Ptr(float64(2))}}).Postgres)
	assert.Equal(t, "numeric(10)[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 10}}).Postgres)
	assert.Equal(t, "smallint[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 16}}).Postgres)
	assert.Equal(t, "int4[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 32}}).Postgres)
	assert.Equal(t, "int8[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeInt, Length: &schema.SchemaJsonTablesElemColumnsElemLength{Precision: 64}}).Postgres)
	assert.Equal(t, "int8[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeInt}).Postgres)
	assert.Equal(t, "text[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeString}).Postgres)
	assert.Equal(t, "varchar(255)[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeString, MaxLength: util.Ptr(int(255))}).Postgres)
	assert.Equal(t, "bytea[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeBinary)}).Postgres)
	assert.Equal(t, "bit[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeBit)}).Postgres)
	assert.Equal(t, "bit(2)[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeBit), MaxLength: util.Ptr(2)}).Postgres)
	assert.Equal(t, "jsonb[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeJson)}).Postgres)
	assert.Equal(t, "uuid[]", *ToNativeType(schema.SchemaJsonTablesElemColumnsElem{IsArray: true, Type: schema.SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(schema.SchemaJsonTablesElemColumnsElemSubtypeUuid)}).Postgres)
}

func assertDataTypeToType(t *testing.T, thetype string, nativeType string, expectType schema.SchemaJsonTablesElemColumnsElemType, expectArray bool) {
	res, array, err := dataTypeToType(thetype, nativeType)
	assert.NoError(t, err)
	assert.Equal(t, expectArray, array)
	assert.Equal(t, expectType, res)
}

func TestDataTypeToType(t *testing.T) {
	assertDataTypeToType(t, "text", "text", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "uuid", "uuid", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "json", "json", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "jsonb", "jsonb", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "xml", "xml", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "cidr", "cidr", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "bit", "bit", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "bit varying", "bit varying", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "bytea", "bytea", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "character", "character", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "character varying", "character varying", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "circle", "circle", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "inet", "inet", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "interval", "interval", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "line", "line", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "lseg", "lseg", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "macaddr", "macaddr", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "macaddr8", "macaddr8", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "path", "path", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "pg_snapshot", "pg_snapshot", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "point", "point", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "polygon", "polygon", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "tsquery", "tsquery", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "tsvector", "tsvector", schema.SchemaJsonTablesElemColumnsElemTypeString, false)
	assertDataTypeToType(t, "txid_snapshot", "txid_snapshot", schema.SchemaJsonTablesElemColumnsElemTypeString, false)

	assertDataTypeToType(t, "integer", "integer", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "int2", "int2", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "int4", "int4", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "int8", "int8", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "bigint", "bigint", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "bigserial", "bigserial", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "pg_lsn", "pg_lsn", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "smallint", "smallint", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "smallserial", "smallserial", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "serial", "serial", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)
	assertDataTypeToType(t, "decimal", "decimal", schema.SchemaJsonTablesElemColumnsElemTypeInt, false)

	assertDataTypeToType(t, "real", "real", schema.SchemaJsonTablesElemColumnsElemTypeFloat, false)
	assertDataTypeToType(t, "double precision", "double precision", schema.SchemaJsonTablesElemColumnsElemTypeFloat, false)
	assertDataTypeToType(t, "money", "money", schema.SchemaJsonTablesElemColumnsElemTypeFloat, false)
	assertDataTypeToType(t, "numeric", "numeric", schema.SchemaJsonTablesElemColumnsElemTypeFloat, false)
	assertDataTypeToType(t, "float4", "float4", schema.SchemaJsonTablesElemColumnsElemTypeFloat, false)
	assertDataTypeToType(t, "float8", "float8", schema.SchemaJsonTablesElemColumnsElemTypeFloat, false)

	assertDataTypeToType(t, "date", "date", schema.SchemaJsonTablesElemColumnsElemTypeDatetime, false)
	assertDataTypeToType(t, "time", "time", schema.SchemaJsonTablesElemColumnsElemTypeDatetime, false)
	assertDataTypeToType(t, "timestamp", "timestamp", schema.SchemaJsonTablesElemColumnsElemTypeDatetime, false)
	assertDataTypeToType(t, "timestamp with time zone", "timestamp with time zone", schema.SchemaJsonTablesElemColumnsElemTypeDatetime, false)
	assertDataTypeToType(t, "timestamp without time zone", "timestamp without time zone", schema.SchemaJsonTablesElemColumnsElemTypeDatetime, false)

	assertDataTypeToType(t, "boolean", "boolean", schema.SchemaJsonTablesElemColumnsElemTypeBoolean, false)

	assertDataTypeToType(t, "ARRAY", "_boolean", schema.SchemaJsonTablesElemColumnsElemTypeBoolean, true)
	assertDataTypeToType(t, "ARRAY", "boolean", schema.SchemaJsonTablesElemColumnsElemTypeBoolean, true)
}

func assertFormatDefault(t *testing.T, dt string, def string, expectValue string) {
	val, err := formatDefault(types.ColumnDetail{DataType: dt, Default: util.Ptr(def)})
	assert.NoError(t, err)
	assert.NotNil(t, val)
	assert.Equal(t, expectValue, *val)
}

func TestFormatDefault(t *testing.T) {
	assertFormatDefault(t, "string", "foo", "foo")
	assertFormatDefault(t, "string", "'foo'::jsonb", "foo")
	assertFormatDefault(t, "string", "'foo'", "foo")

	val, err := formatDefault(types.ColumnDetail{DataType: "int", Default: util.Ptr("a")})
	assert.EqualError(t, err, `invalid int value: a. should be: ^-?\d+(.\d+)?$`)
	assert.Nil(t, val)

	val, err = formatDefault(types.ColumnDetail{DataType: "int", Default: util.Ptr("10")})
	assert.NoError(t, err)
	assert.NotNil(t, val)

	val, err = formatDefault(types.ColumnDetail{DataType: "int", Default: util.Ptr("nextval('playing_with_neon_id_seq'::regclass)")})
	assert.NoError(t, err)
	assert.NotNil(t, val)

	val, err = formatDefault(types.ColumnDetail{DataType: "float", Default: util.Ptr("a")})
	assert.EqualError(t, err, `invalid float value: a. should be: ^-?\d+(.\d+)?$`)
	assert.Nil(t, val)

	val, err = formatDefault(types.ColumnDetail{DataType: "float", Default: util.Ptr("10.1")})
	assert.NoError(t, err)
	assert.NotNil(t, val)

	val, err = formatDefault(types.ColumnDetail{DataType: "boolean", Default: util.Ptr("a")})
	assert.EqualError(t, err, `invalid boolean value: a. should be either true or false`)
	assert.Nil(t, val)

	val, err = formatDefault(types.ColumnDetail{DataType: "boolean", Default: util.Ptr("true")})
	assert.NoError(t, err)
	assert.NotNil(t, val)

	val, err = formatDefault(types.ColumnDetail{DataType: "boolean", Default: util.Ptr("false")})
	assert.NoError(t, err)
	assert.NotNil(t, val)
}

func TestIsFunctionCall(t *testing.T) {
	assert.True(t, util.IsFunctionCall("foo()"))
	assert.True(t, util.IsFunctionCall("nextval('playing_with_neon_id_seq'::regclass)"))
	assert.False(t, util.IsFunctionCall("'foo'"))
	assert.False(t, util.IsFunctionCall("'foo'::jsonb"))
}
