package postgres

import (
	"fmt"
	"io"

	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/schema"
)

type PostgresMigrator struct {
}

var _ migrator.Migrator = (*PostgresMigrator)(nil)
var _ migrator.TableGenerator = (*PostgresMigrator)(nil)

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
	autoIncrements, err := GetTableAutoIncrements(args.Context, args.Logger, args.DB)
	if err != nil {
		return nil, fmt.Errorf("error generating column auto increments: %w", err)
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
			column.UDTName = ToUDTName(column)
			for _, constraint := range detail.Constraints {
				if constraint.Type == "PRIMARY KEY" {
					column.IsPrimaryKey = true
					break
				}
			}
			if columns, ok := autoIncrements[table]; ok {
				if val, ok := columns[column.Name]; ok && val {
					column.IsAutoIncrementing = true
				}
			}
			detail.Columns[i] = column
		}
	}
	return schema.GenerateSchemaJsonFromInfoTables(args.Logger, schema.DatabaseDriverPostgres, tables)
}

// ------------- TableGenerator ------------

func (p *PostgresMigrator) FromSchema(schemajson *schema.SchemaJson, out io.Writer) error {
	for _, table := range schemajson.Tables {
		columns := make([]types.ColumnDetail, 0)
		for i, col := range table.Columns {
			val, err := schema.SchemaColumnToColumn(schema.DatabaseDriverPostgres, col, i+1, ToNativeType(col))
			if err != nil {
				return fmt.Errorf("error creating column: %s for table: %s. %s", col.Name, table.Name, err)
			}
			columns = append(columns, *val)
		}
		statements := migrator.GenerateCreateStatement(table.Name, types.TableDetail{
			Columns:     columns,
			Description: table.Description,
			Constraints: make([]types.ConstraintDetail, 0), // TODO
		}, p)
		io.WriteString(out, statements)
	}
	return nil
}

func (p *PostgresMigrator) QuoteTable(val string) string {
	return quoteIdentifier(val)
}

func (p *PostgresMigrator) QuoteColumn(val string) string {
	return quoteIdentifier(val)
}

func (p *PostgresMigrator) QuoteLiteral(val string) string {
	return quoteValue(val)
}

func (p *PostgresMigrator) GenerateTableComment(table string, val string) string {
	if val == "" {
		return fmt.Sprintf("COMMENT ON TABLE %s IS NULL;", p.QuoteTable(table))
	}
	return fmt.Sprintf("COMMENT ON TABLE %s IS %s;", p.QuoteTable(table), p.QuoteLiteral(val))
}

func (p *PostgresMigrator) GenerateColumnComment(table string, column string, val string) string {
	if val == "" {
		return fmt.Sprintf("COMMENT ON COLUMN %s.%s IS NULL;", p.QuoteTable(table), column)
	}
	return fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s;", p.QuoteTable(table), p.QuoteColumn(column), p.QuoteLiteral(val))
}

func init() {
	var m PostgresMigrator
	migrator.Register("postgres", &m)
	migrator.Register("postgresql", &m)
}
