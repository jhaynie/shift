package migrator

import (
	"context"

	"github.com/jhaynie/shift/internal/schema"
)

type MigrateTableChangeType string
type MigrateColumnChangeType string
type MigrateIndexChangeType string

const (
	CreateTable MigrateTableChangeType = "create table"
	AlterTable  MigrateTableChangeType = "alter table"
	DropTable   MigrateTableChangeType = "drop table"

	CreateColumn MigrateColumnChangeType = "create column"
	AlterColumn  MigrateColumnChangeType = "alter column"
	DropColumn   MigrateColumnChangeType = "drop column"

	CreateIndex MigrateIndexChangeType = "create index"
	AlterIndex  MigrateIndexChangeType = "alter index"
	DropIndex   MigrateIndexChangeType = "drop index"
)

type MigrateColumn struct {
	Change MigrateColumnChangeType
	Name   string // column name
	Type   string // this is the native type, not the schema type
}

type MigrateIndex struct {
	Change  MigrateIndexChangeType
	Name    string   // index name
	Columns []string // columns in the index
}

type MigrateChanges struct {
	Change  MigrateTableChangeType
	Table   string
	Columns []MigrateColumn
	Indexes []MigrateIndex
}

// MigratorCallbackFunc takes a list of changes and returns true if the changes should be applied or false to skip.
type MigratorCallbackFunc func(changes []MigrateChanges) (bool, error)

type Migrator interface {
	// Migrate will compare the schema against the database and apply any necessary changes.
	Migrate(ctx context.Context, dir string, schema *schema.SchemaJson, callback MigratorCallbackFunc) error
}

var migrators map[string]Migrator

// Register is called to register a migrator for a given protocol.
func Register(protocol string, migrator Migrator) {
	if migrators == nil {
		migrators = make(map[string]Migrator)
	}
	migrators[protocol] = migrator
}
