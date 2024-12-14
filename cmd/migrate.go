package cmd

import (
	"context"

	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate [file]",
	Short: "Run the migration",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		dbschema, err := schema.Load(args[0])
		if err != nil {
			logger.Fatal("%s", err)
		}
		drop, _ := cmd.Flags().GetBool("drop")
		db, protocol := connectToDB(cmd, logger, dbschema.Database.Url.(string), drop)
		defer db.Close()
		if err := migrator.Migrate(protocol, migrator.MigratorArgs{
			Context: context.Background(),
			Logger:  logger,
			DB:      db,
			Schema:  dbschema,
			Drop:    drop,
		}); err != nil {
			logger.Fatal("%s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	addUrlFlag(migrateCmd)
	migrateCmd.Flags().Bool("drop", false, "drop the database before migration")
}
