package postgres

import (
	"context"

	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/schema"
)

type PostgresMigrator struct {
}

var _ migrator.Migrator = (*PostgresMigrator)(nil)

func (p *PostgresMigrator) Migrate(ctx context.Context, dir string, schema *schema.SchemaJson, callback migrator.MigratorCallbackFunc) error {
	return nil
}
