package schema

import (
	"encoding/json"
	"testing"

	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/util"
	"github.com/stretchr/testify/assert"
)

func TestLoadYAML(t *testing.T) {
	s, err := Load("../testdata/example1.yaml")
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, "1", s.Version)
	assert.Equal(t, "postgres://localhost:5432/db1", s.Database.Url)
	assert.Len(t, s.Tables, 1)
	assert.Equal(t, "table1", s.Tables[0].Name)
	assert.Len(t, s.Tables[0].Columns, 5)

	assert.Equal(t, "id", s.Tables[0].Columns[0].Name)
	assert.NotNil(t, s.Tables[0].Columns[0].PrimaryKey)
	assert.True(t, *s.Tables[0].Columns[0].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[0].Description)
	assert.Equal(t, "This is a description of id", *s.Tables[0].Columns[0].Description)
	assert.NotNil(t, s.Tables[0].Columns[0].Type)
	assert.Equal(t, "int", string(s.Tables[0].Columns[0].Type))

	assert.Equal(t, "count", s.Tables[0].Columns[1].Name)
	assert.Nil(t, s.Tables[0].Columns[1].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[1].Description)
	assert.Equal(t, "This is a description of count", *s.Tables[0].Columns[1].Description)
	assert.NotNil(t, s.Tables[0].Columns[1].Type)
	assert.Equal(t, "int", string(s.Tables[0].Columns[1].Type))
	assert.NotNil(t, s.Tables[0].Columns[1].Default)
	assert.NotNil(t, s.Tables[0].Columns[1].Default.Postgres)
	assert.Equal(t, "1", string(*s.Tables[0].Columns[1].Default.Postgres))

	assert.Equal(t, "name", s.Tables[0].Columns[2].Name)
	assert.Nil(t, s.Tables[0].Columns[2].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[2].Description)
	assert.Equal(t, "This is a description of name", *s.Tables[0].Columns[2].Description)
	assert.NotNil(t, s.Tables[0].Columns[2].Unique)
	assert.True(t, *s.Tables[0].Columns[2].Unique)
	assert.NotNil(t, s.Tables[0].Columns[2].Type)
	assert.Equal(t, "string", string(s.Tables[0].Columns[2].Type))
	assert.Nil(t, s.Tables[0].Columns[2].Default)

	assert.Equal(t, "uuid", s.Tables[0].Columns[3].Name)
	assert.Nil(t, s.Tables[0].Columns[3].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[3].Description)
	assert.Equal(t, "This is a description of uuid", *s.Tables[0].Columns[3].Description)
	assert.Nil(t, s.Tables[0].Columns[3].Unique)
	assert.NotNil(t, s.Tables[0].Columns[3].Type)
	assert.Equal(t, "string", string(s.Tables[0].Columns[3].Type))
	assert.NotNil(t, s.Tables[0].Columns[3].Subtype)
	assert.Equal(t, "uuid", string(*s.Tables[0].Columns[3].Subtype))
	assert.Nil(t, s.Tables[0].Columns[3].Default)

	assert.Equal(t, "ip_address", s.Tables[0].Columns[4].Name)
	assert.Nil(t, s.Tables[0].Columns[4].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[4].Description)
	assert.Equal(t, "This is a description of ip_address", *s.Tables[0].Columns[4].Description)
	assert.Nil(t, s.Tables[0].Columns[4].Unique)
	assert.NotNil(t, s.Tables[0].Columns[4].Type)
	assert.Equal(t, "string", string(s.Tables[0].Columns[4].Type))
	assert.NotNil(t, s.Tables[0].Columns[4].NativeType)
	assert.NotNil(t, s.Tables[0].Columns[4].NativeType.Postgres)
	assert.Equal(t, "cidr", string(*s.Tables[0].Columns[4].NativeType.Postgres))
	assert.Nil(t, s.Tables[0].Columns[4].Default)
}

func TestLoadJSON(t *testing.T) {
	s1, err := Load("../testdata/example1.yaml")
	assert.NoError(t, err)
	assert.NotNil(t, s1)
	s2, err := Load("../testdata/example1.json")
	assert.NoError(t, err)
	assert.NotNil(t, s2)
	b1, err := json.Marshal(s1)
	assert.NoError(t, err)
	b2, err := json.Marshal(s2)
	assert.NoError(t, err)
	assert.Equal(t, string(b1), string(b2))
}

func TestToNativeType(t *testing.T) {
	assert.Nil(t, ToNativeType(DatabaseDriverPostgres, ""))
	assert.NotNil(t, ToNativeType(DatabaseDriverPostgres, "1"))
	assert.NotNil(t, ToNativeType(DatabaseDriverPostgres, "1").Postgres)
	assert.Equal(t, "1", *ToNativeType(DatabaseDriverPostgres, "1").Postgres)

	assert.Nil(t, ToNativeType(DatabaseDriverSQLite, ""))
	assert.NotNil(t, ToNativeType(DatabaseDriverSQLite, "1"))
	assert.NotNil(t, ToNativeType(DatabaseDriverSQLite, "1").Sqlite)
	assert.Equal(t, "1", *ToNativeType(DatabaseDriverSQLite, "1").Sqlite)

	assert.Nil(t, ToNativeType(DatabaseDriverMysql, ""))
	assert.NotNil(t, ToNativeType(DatabaseDriverMysql, "1"))
	assert.NotNil(t, ToNativeType(DatabaseDriverMysql, "1").Mysql)
	assert.Equal(t, "1", *ToNativeType(DatabaseDriverMysql, "1").Mysql)
}

func TestToNativeDefault(t *testing.T) {
	assert.Nil(t, ToNativeDefault(DatabaseDriverPostgres, nil))
	assert.NotNil(t, ToNativeDefault(DatabaseDriverPostgres, util.Ptr("1")))
	assert.NotNil(t, ToNativeDefault(DatabaseDriverPostgres, util.Ptr("1")).Postgres)
	assert.Equal(t, "1", *ToNativeDefault(DatabaseDriverPostgres, util.Ptr("1")).Postgres)

	assert.Nil(t, ToNativeDefault(DatabaseDriverSQLite, nil))
	assert.NotNil(t, ToNativeDefault(DatabaseDriverSQLite, util.Ptr("1")))
	assert.NotNil(t, ToNativeDefault(DatabaseDriverSQLite, util.Ptr("1")).Sqlite)
	assert.Equal(t, "1", *ToNativeDefault(DatabaseDriverSQLite, util.Ptr("1")).Sqlite)

	assert.Nil(t, ToNativeDefault(DatabaseDriverMysql, nil))
	assert.NotNil(t, ToNativeDefault(DatabaseDriverMysql, util.Ptr("1")))
	assert.NotNil(t, ToNativeDefault(DatabaseDriverMysql, util.Ptr("1")).Mysql)
	assert.Equal(t, "1", *ToNativeDefault(DatabaseDriverMysql, util.Ptr("1")).Mysql)
}

func TestFromNativeType(t *testing.T) {
	assert.Nil(t, FromNativeType(DatabaseDriverPostgres, nil))
	assert.NotNil(t, FromNativeType(DatabaseDriverPostgres, &SchemaJsonTablesElemColumnsElemNativeType{Postgres: util.Ptr("1")}))
	assert.Equal(t, "1", *FromNativeType(DatabaseDriverPostgres, &SchemaJsonTablesElemColumnsElemNativeType{Postgres: util.Ptr("1")}))

	assert.Nil(t, FromNativeType(DatabaseDriverSQLite, nil))
	assert.NotNil(t, FromNativeType(DatabaseDriverSQLite, &SchemaJsonTablesElemColumnsElemNativeType{Sqlite: util.Ptr("1")}))
	assert.Equal(t, "1", *FromNativeType(DatabaseDriverSQLite, &SchemaJsonTablesElemColumnsElemNativeType{Sqlite: util.Ptr("1")}))

	assert.Nil(t, FromNativeType(DatabaseDriverMysql, nil))
	assert.NotNil(t, FromNativeType(DatabaseDriverMysql, &SchemaJsonTablesElemColumnsElemNativeType{Mysql: util.Ptr("1")}))
	assert.Equal(t, "1", *FromNativeType(DatabaseDriverMysql, &SchemaJsonTablesElemColumnsElemNativeType{Mysql: util.Ptr("1")}))
}

func TestFromNativeDefault(t *testing.T) {
	assert.Nil(t, FromNativeDefault(DatabaseDriverPostgres, nil))
	assert.NotNil(t, FromNativeDefault(DatabaseDriverPostgres, &SchemaJsonTablesElemColumnsElemDefault{Postgres: util.Ptr("1")}))
	assert.Equal(t, "1", *FromNativeDefault(DatabaseDriverPostgres, &SchemaJsonTablesElemColumnsElemDefault{Postgres: util.Ptr("1")}))

	assert.Nil(t, FromNativeDefault(DatabaseDriverSQLite, nil))
	assert.NotNil(t, FromNativeDefault(DatabaseDriverSQLite, &SchemaJsonTablesElemColumnsElemDefault{Sqlite: util.Ptr("1")}))
	assert.Equal(t, "1", *FromNativeDefault(DatabaseDriverSQLite, &SchemaJsonTablesElemColumnsElemDefault{Sqlite: util.Ptr("1")}))

	assert.Nil(t, FromNativeDefault(DatabaseDriverMysql, nil))
	assert.NotNil(t, FromNativeDefault(DatabaseDriverMysql, &SchemaJsonTablesElemColumnsElemDefault{Mysql: util.Ptr("1")}))
	assert.Equal(t, "1", *FromNativeDefault(DatabaseDriverMysql, &SchemaJsonTablesElemColumnsElemDefault{Mysql: util.Ptr("1")}))
}

func TestValidateDefaultValue(t *testing.T) {
	assert.NoError(t, validateDefaultValue(types.ColumnDetail{}, SchemaJsonTablesElemColumnsElem{}))
	assert.NoError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr("1")}, SchemaJsonTablesElemColumnsElem{Type: SchemaJsonTablesElemColumnsElemTypeInt}))
	assert.NoError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr("1.2")}, SchemaJsonTablesElemColumnsElem{Type: SchemaJsonTablesElemColumnsElemTypeFloat}))
	assert.NoError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr("1")}, SchemaJsonTablesElemColumnsElem{Type: SchemaJsonTablesElemColumnsElemTypeString}))
	assert.NoError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr(`{"a":"b"}`)}, SchemaJsonTablesElemColumnsElem{Type: SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(SchemaJsonTablesElemColumnsElemSubtypeJson)}))
	assert.NoError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr(`[]`)}, SchemaJsonTablesElemColumnsElem{Type: SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(SchemaJsonTablesElemColumnsElemSubtypeJson)}))

	assert.EqualError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr(`[`)}, SchemaJsonTablesElemColumnsElem{Name: "f", Type: SchemaJsonTablesElemColumnsElemTypeString, Subtype: util.Ptr(SchemaJsonTablesElemColumnsElemSubtypeJson)}), "invalid default json value for column: f")
	assert.EqualError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr("a")}, SchemaJsonTablesElemColumnsElem{Name: "f", Type: SchemaJsonTablesElemColumnsElemTypeInt}), `invalid int default value: a for column: f. should be: ^-?\d+$`)
	assert.EqualError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr("1.0")}, SchemaJsonTablesElemColumnsElem{Name: "f", Type: SchemaJsonTablesElemColumnsElemTypeInt}), `invalid int default value: 1.0 for column: f. should be: ^-?\d+$`)
	assert.EqualError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr("a")}, SchemaJsonTablesElemColumnsElem{Name: "f", Type: SchemaJsonTablesElemColumnsElemTypeFloat}), `invalid float default value: a for column: f. should be: ^-?\d+(.\d+)?$`)
	assert.EqualError(t, validateDefaultValue(types.ColumnDetail{Default: util.Ptr("a")}, SchemaJsonTablesElemColumnsElem{Name: "f", Type: SchemaJsonTablesElemColumnsElemTypeBoolean}), `invalid boolean default value: a for column: f. should be either true or false`)
}
