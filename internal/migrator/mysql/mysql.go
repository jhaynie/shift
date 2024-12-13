package mysql

import (
	"github.com/jhaynie/shift/internal/migrator"
)

type MysqlMigrator struct {
}

var _ migrator.Migrator = (*MysqlMigrator)(nil)

func (p *MysqlMigrator) Migrate(args migrator.MigratorArgs) error {
	return nil
}
