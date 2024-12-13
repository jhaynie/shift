package schema

import (
	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/shopmonkeyus/go-common/logger"
)

func GenerateSchemaJsonFromInfoTables(logger logger.Logger, tables map[string]*types.TableDetail) (*SchemaJson, error) {
	var schemaJson SchemaJson
	schemaJson.Version = DefaultVersion
	schemaJson.Tables = make([]SchemaJsonTablesElem, 0)
	for table, detail := range tables {
		var elem SchemaJsonTablesElem
		elem.Name = table
		elem.Description = detail.Description
		elem.Columns = make([]SchemaJsonTablesElemColumnsElem, len(detail.Columns))
		for i, column := range detail.Columns {
			col := SchemaJsonTablesElemColumnsElem{
				Name:        column.Name,
				Default:     column.Default,
				Description: column.Description,
				Type:        SchemaJsonTablesElemColumnsElemType(column.DataType),
				NativeType:  &column.UDTName,
				Nullable:    &column.IsNullable,
			}
			elem.Columns[i] = col
		}
		schemaJson.Tables = append(schemaJson.Tables, elem)
	}
	return &schemaJson, nil
}
