package postgres

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/jhaynie/shift/internal/util"
)

type PostgresMigrator struct {
}

var _ migrator.Migrator = (*PostgresMigrator)(nil)
var _ migrator.TableGenerator = (*PostgresMigrator)(nil)

func (p *PostgresMigrator) Process(dbschema *schema.SchemaJson) error {
	for _, table := range dbschema.Tables {
		for i, col := range table.Columns {
			col.NativeType = ToNativeType(col)
			// if this is a serial type, we need to set the default to the generated auto increment sequence
			if col.Type == schema.SchemaJsonTablesElemColumnsElemTypeInt && col.AutoIncrement != nil && *col.AutoIncrement && col.Default == nil {
				col.Default = &schema.SchemaJsonTablesElemColumnsElemDefault{
					Postgres: util.Ptr(fmt.Sprintf("nextval('%s_%s_seq'::regclass)", table.Name, col.Name)),
				}
			}
			table.Columns[i] = col
		}
	}
	return nil
}

func (p *PostgresMigrator) Migrate(args migrator.MigratorArgs) error {
	if args.Drop {
		var out strings.Builder
		ts := time.Now()
		if err := p.FromSchema(args.Schema, &out); err != nil {
			return err
		}
		args.Logger.Info("generated sql in %v", time.Since(ts))
		ts = time.Now()
		if _, err := args.DB.ExecContext(args.Context, out.String()); err != nil {
			return err
		}
		args.Logger.Info("executed sql in %v", time.Since(ts))
	}
	return nil
}

func (p *PostgresMigrator) ToSchema(args migrator.ToSchemaArgs) (*schema.SchemaJson, error) {
	tables, err := migrator.GenerateInfoTables(args.Context, args.Logger, args.DB, migrator.WithTableFilter(args.TableFilter))
	if err != nil {
		return nil, fmt.Errorf("error generating table schema: %w", err)
	}
	tableComments, err := getTableDescriptions(args.Context, args.Logger, args.DB)
	if err != nil {
		return nil, fmt.Errorf("error generating table descriptions: %w", err)
	}
	columnComments, err := getColumnDescriptions(args.Context, args.Logger, args.DB)
	if err != nil {
		return nil, fmt.Errorf("error generating column descriptions: %w", err)
	}
	autoIncrements, err := getTableAutoIncrements(args.Context, args.Logger, args.DB)
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
			dt, _, err := dataTypeToType(column.DataType, column.UDTName)
			if err != nil {
				return nil, fmt.Errorf("error converting column %s with table: %s. %s", column.Name, table, err)
			}
			column.DataType = string(dt)
			for _, constraint := range detail.Constraints {
				if constraint.Column == column.Name && constraint.Type == "PRIMARY KEY" {
					column.IsPrimaryKey = true
					break
				}
			}
			if columns, ok := autoIncrements[table]; ok {
				if val, ok := columns[column.Name]; ok && val {
					column.IsAutoIncrementing = true
				}
			}
			column.UDTName, column.IsArray = toUDTName(column)
			column.Default, err = formatDefault(column)
			if err != nil {
				return nil, fmt.Errorf("error validating column: %s table: %s default value: %s", column.Name, table, err)
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

func (p *PostgresMigrator) QuoteDefaultValue(val string, column types.ColumnDetail) string {
	if column.DataType == "string" && !util.IsFunctionCall(val) {
		val = p.QuoteLiteral(val)
		if column.UDTName == "jsonb" && !strings.HasSuffix(val, "::jsonb") {
			val += "::jsonb"
		}
	}
	return val
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
