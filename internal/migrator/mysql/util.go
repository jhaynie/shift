package mysql

import (
	"context"
	"database/sql"
)

var tableCommentSQL = `SELECT
    TABLE_NAME,
    TABLE_COMMENT
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_SCHEMA = database()
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
    TABLE_NAME,
    COLUMN_NAME,
    COLUMN_COMMENT
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    TABLE_SCHEMA = database()
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
