package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/erikgeiser/promptkit"
	"github.com/erikgeiser/promptkit/selection"
	"github.com/jhaynie/shift/internal/diff"
	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/jhaynie/shift/internal/util"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:   "migrate [file]",
	Short: "Run the migration",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		drop, _ := cmd.Flags().GetBool("drop")
		db, protocol, changes, fromSchema, toSchema := rundiff(cmd, logger, args[0], drop)
		defer db.Close()
		if len(changes) == 0 {
			logger.Info("no changes detected")
			return
		}
		confirm, _ := cmd.Flags().GetBool("confirm")
	restart:
		for confirm {
			input := selection.New(fmt.Sprintf("Apply %d database %s? ", len(changes), util.Plural(len(changes), "change", "changes")), []string{"Yes", "Show Diff", "Show SQL", "No"})
			input.Filter = nil // turn off filtering
			ready, err := input.RunPrompt()
			if err != nil && !errors.Is(err, promptkit.ErrAborted) {
				logger.Fatal("%s", err)
			}
			switch ready {
			case "Yes":
				break restart
			case "No":
				return
			case "Show Diff", "Show SQL":
				fmt.Println()
				driver := schema.DatabaseDriverType(protocol)
				format := diff.FormatText
				if ready == "Show SQL" {
					format = diff.FormatSQL
				}
				if err := diff.FormatDiff(format, driver, changes, os.Stdout); err != nil {
					logger.Fatal("%s", err)
				}
				fmt.Println()
			}
		}
		if err := migrator.Migrate(protocol, migrator.MigratorArgs{
			Context:    context.Background(),
			Logger:     logger,
			DB:         db,
			FromSchema: fromSchema,
			ToSchema:   toSchema,
			Diff:       changes,
			Drop:       drop,
		}); err != nil {
			logger.Fatal("%s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	addUrlFlag(migrateCmd)
	migrateCmd.Flags().Bool("drop", false, "drop the database before migration")
	migrateCmd.Flags().Bool("confirm", true, "ask for confirmation before continuing")
}
