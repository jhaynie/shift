package postgres

import (
	"testing"

	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/jhaynie/shift/internal/util"
	"github.com/stretchr/testify/assert"
)

func assertColumnDetail(t *testing.T, expectType string, expectArray bool, detail types.ColumnDetail) {
	val, isArray := ToUDTName(detail)
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
