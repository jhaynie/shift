package mysql

import (
	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/schema"
)

type MysqlMigrator struct {
}

var _ migrator.Migrator = (*MysqlMigrator)(nil)

func (p *MysqlMigrator) Migrate(args migrator.MigratorArgs) error {
	return nil
}

func (p *MysqlMigrator) ToSchema(args migrator.ToSchemaArgs) (*schema.SchemaJson, error) {
	return nil, nil
}

func init() {
	migrator.Register("mysql", &MysqlMigrator{})
}
