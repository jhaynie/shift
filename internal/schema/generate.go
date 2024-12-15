package schema

import (
	"encoding/json"
	"fmt"

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
				IsArray:       column.IsArray,
			}
			if column.MaxLength != nil && *column.MaxLength > 0 {
				col.MaxLength = util.Ptr(int(*column.MaxLength))
			}
			if column.NumericPrecision != nil && *column.NumericPrecision > 0 {
				col.Length = &SchemaJsonTablesElemColumnsElemLength{
					Precision: int(*column.NumericPrecision),
				}
				if column.NumericScale != nil && *column.NumericScale != 0 {
					col.Length.Scale = util.Ptr(float64(*column.NumericScale))
				}
			}
			elem.Columns[i] = col
		}
		schemaJson.Tables = append(schemaJson.Tables, elem)
	}
	return &schemaJson, nil
}

func validateDefaultValue(detail types.ColumnDetail, column SchemaJsonTablesElemColumnsElem) error {
	if detail.Default != nil && !util.IsFunctionCall(*detail.Default) {
		switch column.Type {
		case SchemaJsonTablesElemColumnsElemTypeInt:
			if !util.IsInteger.MatchString(*detail.Default) {
				return fmt.Errorf("invalid %s default value: %s for column: %s. should be: %s", column.Type, *detail.Default, column.Name, util.IsInteger.String())
			}
		case SchemaJsonTablesElemColumnsElemTypeFloat:
			if !util.IsFloat.MatchString(*detail.Default) {
				return fmt.Errorf("invalid %s default value: %s for column: %s. should be: %s", column.Type, *detail.Default, column.Name, util.IsFloat.String())
			}
		case SchemaJsonTablesElemColumnsElemTypeBoolean:
			switch *detail.Default {
			case "true", "false":
			default:
				return fmt.Errorf("invalid boolean default value: %s for column: %s. should be either true or false", *detail.Default, column.Name)
			}
		case SchemaJsonTablesElemColumnsElemTypeString:
			if column.Subtype != nil {
				switch *column.Subtype {
				case SchemaJsonTablesElemColumnsElemSubtypeJson:
					if !json.Valid([]byte(*detail.Default)) {
						return fmt.Errorf("invalid default json value for column: %s", column.Name)
					}
				}
			}
		}
	}
	return nil
}

func SchemaColumnToColumn(driver DatabaseDriverType, column SchemaJsonTablesElemColumnsElem, ordinal int, nativeType *SchemaJsonTablesElemColumnsElemNativeType) (*types.ColumnDetail, error) {
	var detail types.ColumnDetail
	detail.Name = column.Name
	detail.DataType = string(column.Type)
	nt := FromNativeType(driver, nativeType)
	if nt != nil {
		detail.UDTName = *nt
	} else {
		return nil, fmt.Errorf("error generating native type for column %s", column.Name)
	}
	detail.Default = FromNativeDefault(driver, column.Default)
	if err := validateDefaultValue(detail, column); err != nil {
		return nil, err
	}
	detail.Description = column.Description
	if column.AutoIncrement != nil {
		detail.IsAutoIncrementing = *column.AutoIncrement
	}
	if column.Nullable != nil {
		detail.IsNullable = *column.Nullable
	}
	if column.PrimaryKey != nil {
		detail.IsPrimaryKey = *column.PrimaryKey
	}
	if column.Unique != nil {
		detail.IsUnique = *column.Unique
	}
	if column.MaxLength != nil && *column.MaxLength > 0 {
		detail.MaxLength = util.Ptr(int64(*column.MaxLength))
	}
	if column.Length != nil {
		detail.NumericPrecision = util.Ptr(int64(column.Length.Precision))
		if column.Length.Scale != nil && *column.Length.Scale != 0 {
			detail.NumericScale = util.Ptr(int64(*column.Length.Scale))
		}
	}
	detail.Ordinal = int64(ordinal)
	return &detail, nil
}
