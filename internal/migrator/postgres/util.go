package postgres

import (
	"context"
	"database/sql"

	"github.com/jhaynie/shift/internal/schema"
)

var tableCommentSQL = `SELECT
    c.relname,
    obj_description(c.oid)
FROM
    pg_class c
JOIN
    pg_namespace n ON n.oid = c.relnamespace
WHERE
    n.nspname = 'public'
    AND c.relkind = 'r'
`

// GetTableDescriptions will return a map of table to table comment
func GetTableDescriptions(ctx context.Context, db *sql.DB) (map[string]string, error) {
	res, err := db.QueryContext(ctx, tableCommentSQL)
	if err != nil && err != sql.ErrNoRows {
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

var columnCommentSQL = `SELECT
	col.table_name,
	col.column_name,
  pg_catalog.col_description(c.oid, a.attnum)
FROM
	information_schema.columns col
JOIN
	pg_attribute a ON a.attname = col.column_name
JOIN
	pg_class c ON c.oid = a.attrelid
WHERE
	col.table_schema = 'public'
	AND a.attnum > 0
`

// GetColumnDescriptions will return a map of table to a map of column comments
func GetColumnDescriptions(ctx context.Context, db *sql.DB) (map[string]map[string]string, error) {
	res, err := db.QueryContext(ctx, columnCommentSQL)
	if err != nil && err != sql.ErrNoRows {
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
func DataTypeToType(val string) schema.SchemaJsonTablesElemColumnsElemType {
	switch val {
	case "text", "uuid", "json", "jsonb", "xml", "cidr", "bit", "bit varying", "bytea", "character", "character varying", "circle", "inet", "interval", "line", "lseg", "macaddr", "macaddr8", "path", "pg_snapshot", "point", "polygon", "tsquery", "tsvector", "txid_snapshot":
		return schema.SchemaJsonTablesElemColumnsElemTypeString
	case "integer", "bigint", "bigserial", "pg_lsn", "smallint", "smallserial", "serial":
		return schema.SchemaJsonTablesElemColumnsElemTypeInt
	case "real", "double precision", "money", "numeric":
		return schema.SchemaJsonTablesElemColumnsElemTypeFloat
	case "date", "time", "timestamp", "timestamp with time zone", "timestamp without time zone":
		return schema.SchemaJsonTablesElemColumnsElemTypeDatetime
	case "boolean":
		return schema.SchemaJsonTablesElemColumnsElemTypeBoolean
	}
	panic("unhandled data type: " + val)
}
