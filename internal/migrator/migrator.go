package migrator

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jhaynie/shift/internal/schema"
	"github.com/shopmonkeyus/go-common/logger"
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

type MigratorArgs struct {
	Context  context.Context
	Logger   logger.Logger
	Dir      string
	Schema   *schema.SchemaJson
	DB       *sql.DB
	Callback MigratorCallbackFunc
}

type ToSchemaArgs struct {
	Context context.Context
	Logger  logger.Logger
	DB      *sql.DB
}

type Migrator interface {
	// Migrate will compare the schema against the database and apply any necessary changes.
	Migrate(args MigratorArgs) error

	// ToSchema is for generating a schema from a database
	ToSchema(args ToSchemaArgs) (*schema.SchemaJson, error)
}

var migrators map[string]Migrator

// Register is called to register a migrator for a given protocol.
func Register(protocol string, migrator Migrator) {
	if migrators == nil {
		migrators = make(map[string]Migrator)
	}
	migrators[protocol] = migrator
}

func Migrate(protocol string, args MigratorArgs) error {
	migrator := migrators[protocol]
	if migrator == nil {
		return fmt.Errorf("protocol: %s not supported", protocol)
	}
	return migrator.Migrate(args)
}

func ToSchema(protocol string, args ToSchemaArgs) (*schema.SchemaJson, error) {
	migrator := migrators[protocol]
	if migrator == nil {
		return nil, fmt.Errorf("protocol: %s not supported", protocol)
	}
	return migrator.ToSchema(args)
}
