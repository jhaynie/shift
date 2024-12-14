package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/jhaynie/shift/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

func execute(ctx context.Context, logger logger.Logger, db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	logger.Trace("sql: %s", query)
	res, err := db.QueryContext(ctx, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return res, err
}

var tableCommentSQL = util.CleanSQL(`SELECT
    c.relname,
    COALESCE(obj_description(c.oid), '')
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
WHERE
    n.nspname = 'public'
    AND c.relkind = 'r'
		AND c.oid IS NOT NULL
`)

// GetTableDescriptions will return a map of table to table comment
func GetTableDescriptions(ctx context.Context, logger logger.Logger, db *sql.DB) (map[string]string, error) {
	res, err := execute(ctx, logger, db, tableCommentSQL)
	if err != nil {
		return nil, err
	}
	tables := make(map[string]string)
	if res != nil {
		defer res.Close()
		for res.Next() {
			var name, comment string
			if err := res.Scan(&name, &comment); err != nil {
				return nil, err
			}
			tables[name] = comment
		}
	}
	return tables, nil
}

var columnCommentSQL = util.CleanSQL(`SELECT
	col.table_name,
	col.column_name,
  COALESCE(pg_catalog.col_description(c.oid, a.attnum),'')
FROM
	information_schema.columns col
JOIN
	pg_attribute a ON a.attname = col.column_name
JOIN
	pg_class c ON c.oid = a.attrelid
WHERE
	col.table_schema = 'public'
	AND a.attnum > 0
	AND c.oid IS NOT NULL
`)

// GetColumnDescriptions will return a map of table to a map of column comments
func GetColumnDescriptions(ctx context.Context, logger logger.Logger, db *sql.DB) (map[string]map[string]string, error) {
	res, err := execute(ctx, logger, db, columnCommentSQL)
	if err != nil {
		return nil, err
	}
	tables := make(map[string]map[string]string)
	if res != nil {
		defer res.Close()
		for res.Next() {
			var table, column, comment string
			if err := res.Scan(&table, &column, &comment); err != nil {
				return nil, err
			}
			columns := tables[table]
			if columns == nil {
				tables[table] = make(map[string]string)
			}
			if comment != "" {
				tables[table][column] = comment
			}
		}
	}
	return tables, nil
}

// see https://www.postgresql.org/docs/current/datatype.html
func DataTypeToType(val string) (schema.SchemaJsonTablesElemColumnsElemType, error) {
	switch val {
	case "text", "uuid", "json", "jsonb", "xml", "cidr", "bit", "bit varying", "bytea", "character", "character varying", "circle", "inet", "interval", "line", "lseg", "macaddr", "macaddr8", "path", "pg_snapshot", "point", "polygon", "tsquery", "tsvector", "txid_snapshot":
		return schema.SchemaJsonTablesElemColumnsElemTypeString, nil
	case "integer", "bigint", "bigserial", "pg_lsn", "smallint", "smallserial", "serial":
		return schema.SchemaJsonTablesElemColumnsElemTypeInt, nil
	case "real", "double precision", "money", "numeric":
		return schema.SchemaJsonTablesElemColumnsElemTypeFloat, nil
	case "date", "time", "timestamp", "timestamp with time zone", "timestamp without time zone":
		return schema.SchemaJsonTablesElemColumnsElemTypeDatetime, nil
	case "boolean":
		return schema.SchemaJsonTablesElemColumnsElemTypeBoolean, nil
	}
	return "", fmt.Errorf("unhandled data type: %s", val)
}

var tableIdentitySQL = util.CleanSQL(`SELECT
	table_name,
	column_name
FROM
    information_schema.columns
WHERE
	data_type = 'integer'
	AND (is_identity = 'YES' OR column_default LIKE 'nextval%')
	AND table_name IN (
  	SELECT table_name FROM information_schema.tables 
  	WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema') 
  	AND table_catalog = current_database() 
	)`)

// GetTableAutoIncrements returns a map of table to column of those columns which are auto incrementing
func GetTableAutoIncrements(ctx context.Context, logger logger.Logger, db *sql.DB) (map[string]map[string]bool, error) {
	res, err := execute(ctx, logger, db, tableIdentitySQL)
	if err != nil {
		return nil, err
	}
	tables := make(map[string]map[string]bool)
	if res != nil {
		defer res.Close()
		for res.Next() {
			var name, column string
			if err := res.Scan(&name, &column); err != nil {
				return nil, err
			}
			kv := tables[name]
			if kv == nil {
				kv = make(map[string]bool)
				tables[name] = kv
			}
			kv[column] = true
		}
	}
	return tables, nil
}

func ToNativeType(column schema.SchemaJsonTablesElemColumnsElem) *schema.SchemaJsonTablesElemColumnsElemNativeType {
	if column.NativeType != nil && column.NativeType.Postgres != nil {
		return column.NativeType
	}
	switch column.Type {
	case schema.SchemaJsonTablesElemColumnsElemTypeBoolean:
		return schema.ToNativeType(schema.DatabaseDriverPostgres, "boolean")
	case schema.SchemaJsonTablesElemColumnsElemTypeDatetime:
		return schema.ToNativeType(schema.DatabaseDriverPostgres, "timestamp with time zone")
	case schema.SchemaJsonTablesElemColumnsElemTypeFloat:
		return schema.ToNativeType(schema.DatabaseDriverPostgres, "double precision")
	case schema.SchemaJsonTablesElemColumnsElemTypeInt:
		if column.MaxLength != nil && *column.MaxLength > 0 {
			return schema.ToNativeType(schema.DatabaseDriverPostgres, fmt.Sprintf("numeric(%d)", *column.MaxLength))
		}
		if column.Length != nil {
			if column.Length.Scale != nil {
				return schema.ToNativeType(schema.DatabaseDriverPostgres, fmt.Sprintf("numeric(%d,%s)", column.Length.Precision, strconv.FormatFloat(*column.Length.Scale, 'f', 0, 32)))
			}
			return schema.ToNativeType(schema.DatabaseDriverPostgres, fmt.Sprintf("numeric(%d)", column.Length.Precision))
		}
		return schema.ToNativeType(schema.DatabaseDriverPostgres, "bigint")
	case schema.SchemaJsonTablesElemColumnsElemTypeString:
		if column.Subtype != nil {
			switch *column.Subtype {
			case schema.SchemaJsonTablesElemColumnsElemSubtypeArray:
				// TODO
			case schema.SchemaJsonTablesElemColumnsElemSubtypeBinary:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, "bytea")
			case schema.SchemaJsonTablesElemColumnsElemSubtypeBit:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, "bit")
			case schema.SchemaJsonTablesElemColumnsElemSubtypeJson:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, "jsonb")
			case schema.SchemaJsonTablesElemColumnsElemSubtypeUuid:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, "uuid")
			}
		}
		if column.MaxLength != nil && *column.MaxLength > 0 {
			return schema.ToNativeType(schema.DatabaseDriverPostgres, fmt.Sprintf("varchar(%d)", *column.MaxLength))
		}
		return schema.ToNativeType(schema.DatabaseDriverPostgres, "text")
	}
	return nil
}

func ToUDTName(column types.ColumnDetail) string {
	if column.MaxLength != nil && *column.MaxLength > 0 {
		return column.UDTName + fmt.Sprintf("(%d)", *column.MaxLength)
	} else if column.NumericPrecision != nil {
		if column.DataType == "int" && (*column.NumericPrecision == 64 || *column.NumericPrecision == 32) && (column.NumericScale == nil || *column.NumericScale == 0) {
			// this is a normal int
		} else if column.DataType == "float" && (*column.NumericPrecision == 64 || *column.NumericPrecision == 24) && (column.NumericScale == nil || *column.NumericScale == 0) {
			// this is a normal float
		} else {
			if column.DataType == "float" && column.UDTName == "float8" && column.NumericScale == nil && *column.NumericPrecision == 53 {
				// this is double precision type
				return "double precision"
			} else {
				// this is an abitrary number
				if column.NumericScale != nil {
					return column.UDTName + fmt.Sprintf("(%d,%d)", *column.NumericPrecision, *column.NumericScale)
				} else {
					return column.UDTName + fmt.Sprintf("(%d)", *column.NumericPrecision)
				}
			}
		}
	}
	return column.UDTName
}
