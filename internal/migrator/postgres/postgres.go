package postgres

import (
	"fmt"

	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/schema"
)

type PostgresMigrator struct {
}

var _ migrator.Migrator = (*PostgresMigrator)(nil)

func (p *PostgresMigrator) Migrate(args migrator.MigratorArgs) error {
	return nil
}

func (p *PostgresMigrator) ToSchema(args migrator.ToSchemaArgs) (*schema.SchemaJson, error) {
	tables, err := migrator.GenerateInfoTables(args.Context, args.Logger, args.DB)
	if err != nil {
		return nil, fmt.Errorf("error generating table schema: %w", err)
	}
	tableComments, err := GetTableDescriptions(args.Context, args.Logger, args.DB)
	if err != nil {
		return nil, fmt.Errorf("error generating table descriptions: %w", err)
	}
	columnComments, err := GetColumnDescriptions(args.Context, args.Logger, args.DB)
	if err != nil {
		return nil, fmt.Errorf("error generating column descriptions: %w", err)
	}
	for table, detail := range tables {
		if tableComment, ok := tableComments[table]; ok && tableComment != "" {
			detail.Description = &tableComment
		}
		if comments, ok := columnComments[table]; ok {
			for i, column := range detail.Columns {
				if columnComment, ok := comments[column.Name]; ok && columnComment != "" {
					column.Description = &columnComment
					detail.Columns[i] = column
				}
			}
		}
		for i, column := range detail.Columns {
			dt, err := DataTypeToType(column.DataType)
			if err != nil {
				return nil, fmt.Errorf("error converting column %s with table: %s. %s", column.Name, table, err)
			}
			column.DataType = string(dt)
			for _, constraint := range detail.Constraints {
				if constraint.Type == "PRIMARY KEY" {
					column.IsPrimaryKey = true
					break
				}
			}
			detail.Columns[i] = column
		}
	}
	return schema.GenerateSchemaJsonFromInfoTables(args.Logger, tables)
}

func init() {
	migrator.Register("postgresql", &PostgresMigrator{})
}
