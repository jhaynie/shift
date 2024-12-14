package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

var infoTablesQuery = `SELECT
	table_name,
	column_name,
	ordinal_position,
	column_default,
	is_nullable,
	data_type,
	character_maximum_length,
	numeric_precision,
	numeric_scale,
	udt_name
FROM
	information_schema.columns
WHERE
	table_name IN (
		SELECT
			table_name
		FROM
			information_schema.tables
		WHERE
			table_type = '%s'
			AND table_schema NOT IN (%s)
			AND table_catalog = %s
	)
ORDER BY table_name, ordinal_position`

var infoConstraintsQuery = `SELECT
	constraint_name,
	table_name,
	constraint_type
FROM
	information_schema.table_constraints
WHERE
	table_schema NOT IN (%s)
	AND table_catalog = %s
	AND constraint_type != 'CHECK'
ORDER BY table_name`

type infoQueryConfig struct {
	extraSchemaExcludes  []string
	tableTypeOverride    string
	tableCatalogOverride string
	filterTables         []string
}

type WithOption func(config *infoQueryConfig)

var defaultTableExcludes = []string{"pg_catalog", "information_schema"}
var defaultBaseTableTable = "BASE TABLE"
var defaultTableCatalog = "current_database()"

func singleQuote(val string) string {
	return `'` + val + `'`
}

func mapSingleQuote(val []string) []string {
	res := make([]string, len(val))
	for i, s := range val {
		res[i] = singleQuote(s)
	}
	return res
}

func generateInfoTableQuery(config *infoQueryConfig) string {
	excludes := append(append([]string{}, defaultTableExcludes...), config.extraSchemaExcludes...)
	tableCatalog := defaultTableCatalog
	override := defaultBaseTableTable
	if config.tableTypeOverride != "" {
		override = config.tableTypeOverride
	}
	if config.tableCatalogOverride != "" {
		tableCatalog = config.tableCatalogOverride
		if !strings.Contains(tableCatalog, "()") {
			tableCatalog = singleQuote(tableCatalog)
		}
	}
	return util.CleanSQL(fmt.Sprintf(infoTablesQuery, override, strings.Join(mapSingleQuote(excludes), ","), tableCatalog))
}

func generateInfoTableConstraintsQuery(config *infoQueryConfig) string {
	excludes := append(append([]string{}, defaultTableExcludes...), config.extraSchemaExcludes...)
	tableCatalog := defaultTableCatalog
	if config.tableCatalogOverride != "" {
		tableCatalog = config.tableCatalogOverride
		if !strings.Contains(tableCatalog, "()") {
			tableCatalog = singleQuote(tableCatalog)
		}
	}
	return util.CleanSQL(fmt.Sprintf(infoConstraintsQuery, strings.Join(mapSingleQuote(excludes), ","), tableCatalog))
}

func generateDefaultInfoQueryConfig() *infoQueryConfig {
	var config infoQueryConfig
	return &config
}

// WithTableCatalog allows settings an override for the table_catalog predicate which defaults to current_database()
func WithTableCatalog(val string) WithOption {
	return func(config *infoQueryConfig) {
		config.tableCatalogOverride = val
	}
}

// WithTableType allows setting an override for the table_type predicate which defaults to BASE TABLE
func WithTableType(val string) WithOption {
	return func(config *infoQueryConfig) {
		config.tableTypeOverride = val
	}
}

// WithTableSchemaExcludes allows adding additional tables to the table_schema predicate for filtering out table schemas
func WithTableSchemaExcludes(tables []string) WithOption {
	return func(config *infoQueryConfig) {
		config.extraSchemaExcludes = tables
	}
}

// WithTableFilter allows filtering for specific tables
func WithTableFilter(tables []string) WithOption {
	return func(config *infoQueryConfig) {
		config.filterTables = tables
	}
}

// GenerateInfoTables is a utility for generating generic tables from an database that supports the information_schema standard
func GenerateInfoTables(ctx context.Context, logger logger.Logger, db *sql.DB, opts ...WithOption) (map[string]*types.TableDetail, error) {
	config := generateDefaultInfoQueryConfig()
	for _, opt := range opts {
		opt(config)
	}
	querysql := generateInfoTableQuery(config)
	logger.Trace("sql: %s", querysql)
	res, err := db.QueryContext(ctx, querysql)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	tables := make(map[string]*types.TableDetail)
	if res != nil {
		defer res.Close()
		for res.Next() {
			var tableName, columnName, dataType, nullable, udtName string
			var columnDefault sql.NullString
			var maxLength, numericPrecision, numericScale sql.NullInt64
			var ordinal int64
			if err := res.Scan(&tableName, &columnName, &ordinal, &columnDefault, &nullable, &dataType, &maxLength, &numericPrecision, &numericScale, &udtName); err != nil {
				return nil, err
			}
			if len(config.filterTables) > 0 && !util.Contains(config.filterTables, tableName) {
				continue // skip if we're filtering tables
			}
			table := tables[tableName]
			if table == nil {
				table = &types.TableDetail{
					Columns:     make([]types.ColumnDetail, 0),
					Constraints: make([]types.ConstraintDetail, 0),
				}
				tables[tableName] = table
			}
			var detail types.ColumnDetail
			detail.Name = columnName
			detail.Ordinal = ordinal
			if columnDefault.Valid {
				detail.Default = &columnDefault.String
			}
			detail.IsNullable = nullable == "YES"
			detail.DataType = dataType
			detail.UDTName = udtName
			if maxLength.Valid {
				detail.MaxLength = &maxLength.Int64
			}
			if numericPrecision.Valid {
				detail.NumericPrecision = &numericPrecision.Int64
			}
			if numericScale.Valid {
				detail.NumericScale = &numericScale.Int64
			}
			table.Columns = append(table.Columns, detail)
		}
	}
	if len(tables) > 0 {
		constraintQuery := generateInfoTableConstraintsQuery(config)
		logger.Trace("sql: %s", constraintQuery)
		cres, err := db.QueryContext(ctx, constraintQuery)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		if cres != nil {
			defer cres.Close()
			for cres.Next() {
				var name, tablename, ctype string
				if err := cres.Scan(&name, &tablename, &ctype); err != nil {
					return nil, err
				}
				table := tables[tablename]
				if table != nil {
					table.Constraints = append(table.Constraints, types.ConstraintDetail{
						Name: name,
						Type: ctype,
					})
				}
			}
		}
	}
	return tables, nil
}

type TableGenerator interface {
	QuoteTable(val string) string
	QuoteColumn(val string) string
	QuoteLiteral(val string) string
	QuoteDefaultValue(val string, column types.ColumnDetail) string
	GenerateTableComment(table string, val string) string
	GenerateColumnComment(table string, column string, val string) string
}

func GenerateCreateStatement(name string, table types.TableDetail, generator TableGenerator) string {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE IF NOT EXISTS ")
	sql.WriteString(generator.QuoteTable(name))
	sql.WriteString(" (\n")
	var unique []string
	for _, column := range table.Columns {
		if column.IsUnique {
			unique = append(unique, column.Name)
		}
	}
	for i, column := range table.Columns {
		sql.WriteString("   ")
		sql.WriteString(generator.QuoteColumn(column.Name))
		sql.WriteString(" ")
		sql.WriteString(column.UDTName)
		var attrs []string
		if !column.IsNullable && column.Default == nil {
			attrs = append(attrs, "NOT NULL")
		}
		if column.Default != nil {
			val := generator.QuoteDefaultValue(*column.Default, column)
			attrs = append(attrs, "DEFAULT "+val)
		}
		if column.IsUnique && len(unique) <= 1 {
			attrs = append(attrs, "UNIQUE")
		}
		if column.IsPrimaryKey {
			attrs = append(attrs, "PRIMARY KEY")
		}
		if len(attrs) > 0 {
			sql.WriteString(" ")
			sql.WriteString(strings.Join(attrs, " "))
		}
		if i+1 < len(table.Columns) || len(unique) > 1 {
			sql.WriteString(",\n")
		} else {
			sql.WriteString("\n")
		}
	}
	if len(unique) > 1 {
		sql.WriteString(fmt.Sprintf("\tUNIQUE (%s)\n", strings.Join(unique, ",")))
	}
	sql.WriteString(");\n")
	if table.Description != nil {
		sql.WriteString(generator.GenerateTableComment(name, *table.Description))
		sql.WriteString("\n")
	}
	for _, column := range table.Columns {
		if column.Description != nil {
			sql.WriteString(generator.GenerateColumnComment(name, column.Name, *column.Description))
			sql.WriteString("\n")
		}
	}
	return sql.String()
}
