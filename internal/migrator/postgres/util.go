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
func DataTypeToType(val string, nativeType string) (schema.SchemaJsonTablesElemColumnsElemType, bool, error) {
	switch val {
	case "text", "uuid", "json", "jsonb", "xml", "cidr", "bit", "bit varying", "bytea", "character", "character varying", "circle", "inet", "interval", "line", "lseg", "macaddr", "macaddr8", "path", "pg_snapshot", "point", "polygon", "tsquery", "tsvector", "txid_snapshot":
		return schema.SchemaJsonTablesElemColumnsElemTypeString, false, nil
	case "integer", "int2", "int4", "int8", "bigint", "bigserial", "pg_lsn", "smallint", "smallserial", "serial", "decimal":
		return schema.SchemaJsonTablesElemColumnsElemTypeInt, false, nil
	case "real", "double precision", "money", "numeric", "float4", "float8":
		return schema.SchemaJsonTablesElemColumnsElemTypeFloat, false, nil
	case "date", "time", "timestamp", "timestamp with time zone", "timestamp without time zone":
		return schema.SchemaJsonTablesElemColumnsElemTypeDatetime, false, nil
	case "boolean":
		return schema.SchemaJsonTablesElemColumnsElemTypeBoolean, false, nil
	case "ARRAY":
		dt := nativeType
		if dt[0:1] == "_" {
			dt = dt[1:]
		}
		r, _, err := DataTypeToType(dt, "")
		return r, true, err
	}
	return "", false, fmt.Errorf("unhandled data type: %s", val)
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

func toMaybeArray(val string, isArray bool) string {
	if isArray {
		return val + "[]"
	}
	return val
}

func ToNativeType(column schema.SchemaJsonTablesElemColumnsElem) *schema.SchemaJsonTablesElemColumnsElemNativeType {
	if column.NativeType != nil && column.NativeType.Postgres != nil {
		return column.NativeType
	}
	switch column.Type {
	case schema.SchemaJsonTablesElemColumnsElemTypeBoolean:
		return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("boolean", column.IsArray))
	case schema.SchemaJsonTablesElemColumnsElemTypeDatetime:
		return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("timestamp with time zone", column.IsArray))
	case schema.SchemaJsonTablesElemColumnsElemTypeFloat:
		if column.MaxLength != nil && *column.MaxLength == 32 {
			return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("real", column.IsArray))
		}
		return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("double precision", column.IsArray))
	case schema.SchemaJsonTablesElemColumnsElemTypeInt:
		if column.MaxLength != nil && *column.MaxLength > 0 {
			return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray(fmt.Sprintf("numeric(%d)", *column.MaxLength), column.IsArray))
		}
		if column.Length != nil {
			if column.Length.Scale != nil {
				return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray(fmt.Sprintf("numeric(%d,%s)", column.Length.Precision, strconv.FormatFloat(*column.Length.Scale, 'f', 0, 32)), column.IsArray))
			}
			switch column.Length.Precision {
			case 16:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("smallint", column.IsArray))
			case 32:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("int4", column.IsArray))
			case 64:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("int8", column.IsArray))
			}
			return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray(fmt.Sprintf("numeric(%d)", column.Length.Precision), column.IsArray))
		}
		return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("int8", column.IsArray))
	case schema.SchemaJsonTablesElemColumnsElemTypeString:
		if column.Subtype != nil {
			switch *column.Subtype {
			case schema.SchemaJsonTablesElemColumnsElemSubtypeBinary:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("bytea", column.IsArray))
			case schema.SchemaJsonTablesElemColumnsElemSubtypeBit:
				if column.MaxLength != nil && *column.MaxLength > 0 {
					return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray(fmt.Sprintf("bit(%d)", *column.MaxLength), column.IsArray))
				}
				return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("bit", column.IsArray))
			case schema.SchemaJsonTablesElemColumnsElemSubtypeJson:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("jsonb", column.IsArray))
			case schema.SchemaJsonTablesElemColumnsElemSubtypeUuid:
				return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("uuid", column.IsArray))
			}
		}
		if column.MaxLength != nil && *column.MaxLength > 0 {
			return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray(fmt.Sprintf("varchar(%d)", *column.MaxLength), column.IsArray))
		}
		return schema.ToNativeType(schema.DatabaseDriverPostgres, toMaybeArray("text", column.IsArray))
	}
	return nil
}

func ToUDTName(column types.ColumnDetail) (string, bool) {
	val := column.UDTName
	if column.MaxLength != nil && *column.MaxLength > 0 {
		val = column.UDTName + fmt.Sprintf("(%d)", *column.MaxLength)
	} else if column.NumericPrecision != nil {
		switch {
		case column.DataType == "int" && (*column.NumericPrecision == 64 || *column.NumericPrecision == 32) && (column.NumericScale == nil || *column.NumericScale == 0):
			// this is a normal int
			break
		case column.DataType == "int" && *column.NumericPrecision == 16:
			// this is a small int
			val = "int2"
		case column.DataType == "float" && (*column.NumericPrecision == 64 || *column.NumericPrecision == 24) && (column.NumericScale == nil || *column.NumericScale == 0):
			// this is a normal float
			break
		case column.DataType == "float" && column.UDTName == "float8" && column.NumericScale == nil && *column.NumericPrecision == 53:
			// this is double precision type
			val = "double precision"
		case column.NumericScale != nil:
			// this is an abitrary number with scale
			val = column.UDTName + fmt.Sprintf("(%d,%d)", *column.NumericPrecision, *column.NumericScale)
		default:
			// this is an abitrary number
			val = column.UDTName + fmt.Sprintf("(%d)", *column.NumericPrecision)
		}
	}
	// if the type starts with underscore, its an array
	if val != "" && val[0:1] == "_" {
		return val[1:] + "[]", true
	}
	return val, false
}
