package postgres

import (
	"github.com/jhaynie/shift/internal/migrator"
)

type PostgresMigrator struct {
}

var _ migrator.Migrator = (*PostgresMigrator)(nil)

func (p *PostgresMigrator) Migrate(args migrator.MigratorArgs) error {
	return nil
}
