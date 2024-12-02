package diff

import (
	"strings"

	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/schema"
)

type Column struct {
	Name          string  // column name
	Type          string  // generic type
	Subtype       *string // sub type (if needed, otherwise nil)
	NativeType    string  // native database type
	MaxLength     *int    // max length of the column if applicable, otherwise nil
	PrimaryKey    bool    // if this column is part of the primary key
	NotNull       bool    // if this column is nullable
	Unique        bool    // if this column must be unique
	AutoIncrement bool    // if this column auto increments
	Default       *string // if the column has a default value, otherwise nil
}

type IndexOrder string

const (
	IndexOrderAsc  IndexOrder = "asc"
	IndexOrderDesc IndexOrder = "desc"
)

type Index struct {
	Name    string     // name of the index
	Columns []string   // columns in the index
	Order   IndexOrder // order of the index
	Unique  bool       // if the index is unique
}

type CascadeAction string

const (
	CascadeActionNone   CascadeAction = "none"
	CascadeActionUpdate CascadeAction = "update"
	CascadeActionDelete CascadeAction = "delete"
)

type ForeignKey struct {
	Name           string        // name of the foreign key
	Column         []string      // referenced columns
	RefTable       string        // table that is being referenced
	RefColumns     []string      // columns in the referenced table
	OnUpdateAction CascadeAction // action on update
	OnDeleteAction CascadeAction // action on delete
}

type Table struct {
	Name        string       // table name
	ForeignKeys []ForeignKey // foreign keys
	Column      []Column     // columns
	Indexes     []Index      // indexes
}

func createNewColumns(table schema.SchemaJsonTablesElem) []migrator.MigrateColumn {
	var res []migrator.MigrateColumn
	for _, col := range table.Columns {
		res = append(res, migrator.MigrateColumn{
			Change: migrator.CreateColumn,
			Name:   col.Name,
			Type:   *col.NativeType,
		})
	}
	return res
}

func getIndexName(table string, column string) string {
	return "idx_" + strings.ToLower(table) + "_" + strings.ToLower(column)
}

func createNewIndexes(table schema.SchemaJsonTablesElem) []migrator.MigrateIndex {
	var res []migrator.MigrateIndex
	for _, col := range table.Columns {
		if col.Index != nil && *col.Index {
			res = append(res, migrator.MigrateIndex{
				Change:  migrator.CreateIndex,
				Columns: []string{col.Name},
				Name:    getIndexName(table.Name, col.Name),
			})
		}
	}
	return res
}

// Diff will take a set of tables and a schema and return a list of changes that need to be applied to the database.
func Diff(tables map[string]Table, schema *schema.SchemaJson) ([]migrator.MigrateChanges, error) {
	processedTables := make(map[string]bool)
	var res []migrator.MigrateChanges
	for _, table := range schema.Tables {
		if ref, ok := tables[table.Name]; ok {
			// compare the tables
			processedTables[table.Name] = true
			_ = ref
			// TODO
		} else {
			// create the table
			res = append(res, migrator.MigrateChanges{
				Change:  migrator.CreateTable,
				Table:   table.Name,
				Columns: createNewColumns(table),
				Indexes: createNewIndexes(table),
			})
		}
	}
	for _, table := range tables {
		if _, ok := processedTables[table.Name]; !ok {
			// drop the table
			res = append(res, migrator.MigrateChanges{
				Change: migrator.DropTable,
				Table:  table.Name,
			})
		}
	}
	return res, nil
}
