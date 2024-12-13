package schema

import (
	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type DatabaseDriverType string

const (
	DatabaseDriverPostgres DatabaseDriverType = "postgres"
	DatabaseDriverSQLite   DatabaseDriverType = "sqlite"
	DatabaseDriverMysql    DatabaseDriverType = "mysql"
)

func ToNativeType(driver DatabaseDriverType, val string) *SchemaJsonTablesElemColumnsElemNativeType {
	if val != "" {
		switch driver {
		case DatabaseDriverPostgres:
			return &SchemaJsonTablesElemColumnsElemNativeType{Postgres: &val}
		case DatabaseDriverSQLite:
			return &SchemaJsonTablesElemColumnsElemNativeType{Sqlite: &val}
		case DatabaseDriverMysql:
			return &SchemaJsonTablesElemColumnsElemNativeType{Mysql: &val}
		}
	}
	return nil
}

func ToNativeDefault(driver DatabaseDriverType, val *string) *SchemaJsonTablesElemColumnsElemDefault {
	if val != nil {
		switch driver {
		case DatabaseDriverPostgres:
			return &SchemaJsonTablesElemColumnsElemDefault{Postgres: val}
		case DatabaseDriverSQLite:
			return &SchemaJsonTablesElemColumnsElemDefault{Sqlite: val}
		case DatabaseDriverMysql:
			return &SchemaJsonTablesElemColumnsElemDefault{Mysql: val}
		}
	}
	return nil
}

func FromNativeType(driver DatabaseDriverType, val *SchemaJsonTablesElemColumnsElemNativeType) *string {
	if val != nil {
		switch driver {
		case DatabaseDriverPostgres:
			return val.Postgres
		case DatabaseDriverSQLite:
			return val.Sqlite
		case DatabaseDriverMysql:
			return val.Mysql
		}
	}
	return nil
}

func FromNativeDefault(driver DatabaseDriverType, val *SchemaJsonTablesElemColumnsElemDefault) *string {
	if val != nil {
		switch driver {
		case DatabaseDriverPostgres:
			return val.Postgres
		case DatabaseDriverSQLite:
			return val.Sqlite
		case DatabaseDriverMysql:
			return val.Mysql
		}
	}
	return nil
}

func GenerateSchemaJsonFromInfoTables(logger logger.Logger, driver DatabaseDriverType, tables map[string]*types.TableDetail) (*SchemaJson, error) {
	var schemaJson SchemaJson
	schemaJson.Schema = DefaultSchema
	schemaJson.Version = DefaultVersion
	schemaJson.Database.Url = "${DATABASE_URL}"
	schemaJson.Tables = make([]SchemaJsonTablesElem, 0)
	for table, detail := range tables {
		var elem SchemaJsonTablesElem
		elem.Name = table
		elem.Description = detail.Description
		elem.Columns = make([]SchemaJsonTablesElemColumnsElem, len(detail.Columns))
		for i, column := range detail.Columns {
			col := SchemaJsonTablesElemColumnsElem{
				Name:          column.Name,
				Default:       ToNativeDefault(driver, column.Default),
				Description:   column.Description,
				Type:          SchemaJsonTablesElemColumnsElemType(column.DataType),
				NativeType:    ToNativeType(driver, column.UDTName),
				Nullable:      &column.IsNullable,
				AutoIncrement: util.Ptr(column.IsAutoIncrementing),
				PrimaryKey:    util.Ptr(column.IsPrimaryKey),
			}
			if column.MaxLength != nil && *column.MaxLength > 0 {
				col.MaxLength = util.Ptr(int(*column.MaxLength))
			}
			elem.Columns[i] = col
		}
		schemaJson.Tables = append(schemaJson.Tables, elem)
	}
	return &schemaJson, nil
}
