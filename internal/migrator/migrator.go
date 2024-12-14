package migrator

import (
	"context"
	"database/sql"
	"fmt"
	"io"

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
	Change     MigrateColumnChangeType
	Name       string // column name
	Ref        schema.SchemaJsonTablesElemColumnsElem
	Previous   schema.SchemaJsonTablesElemColumnsElem
	ChangeType string // what is changing, the type, etc
	ChangeFrom string // what its going from
	ChangeTo   string /// what its going to
}

type MigrateIndex struct {
	Change  MigrateIndexChangeType
	Name    string   // index name
	Columns []string // columns in the index
}

type MigrateChanges struct {
	Change  MigrateTableChangeType
	Table   string
	Ref     schema.SchemaJsonTablesElem
	Columns []MigrateColumn
	Indexes []MigrateIndex
}

type MigratorArgs struct {
	Context context.Context
	Logger  logger.Logger
	Schema  *schema.SchemaJson
	DB      *sql.DB
	Drop    bool
}

type ToSchemaArgs struct {
	Context     context.Context
	Logger      logger.Logger
	DB          *sql.DB
	TableFilter []string
}

type Migrator interface {
	// Process a schema after loading it
	Process(schema *schema.SchemaJson) error

	// Migrate will compare the schema against the database and apply any necessary changes.
	Migrate(args MigratorArgs) error

	// ToSchema is for generating a schema from a database.
	ToSchema(args ToSchemaArgs) (*schema.SchemaJson, error)

	// FromSchema is for generating a set of SQL from a schema (regardless of the targets current schema).
	FromSchema(schema *schema.SchemaJson, out io.Writer) error
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

func FromSchema(protocol string, schema *schema.SchemaJson, out io.Writer) error {
	migrator := migrators[protocol]
	if migrator == nil {
		return fmt.Errorf("protocol: %s not supported", protocol)
	}
	return migrator.FromSchema(schema, out)
}

func Load(filename string) (*schema.SchemaJson, error) {
	dbschema, err := schema.Load(filename)
	if err != nil {
		return nil, err
	}
	_, protocol, err := DriverFromURL(dbschema.Database.Url.(string))
	if err != nil {
		return nil, fmt.Errorf("error determining protocol from database url: %s", err)
	}
	migrator := migrators[protocol]
	if migrator == nil {
		return nil, fmt.Errorf("protocol: %s not supported", protocol)
	}
	err = migrator.Process(dbschema)
	return dbschema, err
}
