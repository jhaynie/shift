package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jhaynie/shift/internal/diff"
	"github.com/jhaynie/shift/internal/migrator"
	_ "github.com/jhaynie/shift/internal/migrator/mysql"
	_ "github.com/jhaynie/shift/internal/migrator/postgres"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/shopmonkeyus/go-common/logger"
	csys "github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate output",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var generateSchemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Generate schema from an existing database",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		db, protocol := connectToDB(cmd, logger, "", false)
		defer db.Close()
		tables, _ := cmd.Flags().GetStringSlice("table")
		dbschema, err := migrator.ToSchema(protocol, migrator.ToSchemaArgs{
			Context:     context.Background(),
			DB:          db,
			Logger:      logger,
			TableFilter: tables,
		})
		if err != nil {
			logger.Fatal("error generating schema: %s", err)
		}
		outSchema := schema.SchemaJsonForOutput{
			Schema:   dbschema.Schema,
			Version:  dbschema.Version,
			Database: dbschema.Database,
			Tables:   dbschema.Tables,
		}
		format, _ := cmd.Flags().GetString("format")
		var buf []byte
		switch format {
		case "yaml", "yml":
			buf, err = yaml.Marshal(outSchema)
			if err == nil {
				buf = []byte("# yaml-language-server: $schema=" + schema.DefaultSchema + "\n" + string(buf))
			}
		default:
			buf, err = json.MarshalIndent(outSchema, " ", "  ")
		}
		if err != nil {
			logger.Fatal("serialization error: %s", err)
		}
		fmt.Print(string(buf))
	},
}

var generateSQLCmd = &cobra.Command{
	Use:   "sql [file]",
	Args:  cobra.ExactArgs(1),
	Short: "Generate SQL from a schema",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		file := args[0]
		if !csys.Exists(file) {
			logger.Fatal("file %s does not exists or is not accessible", file)
		}
		jsonschema, err := schema.Load(file)
		if err != nil {
			logger.Fatal("%s", err)
		}
		u, err := url.Parse(jsonschema.Database.Url.(string))
		if err != nil {
			logger.Error("error parsing: %s. %s", jsonschema.Database.Url, err)
		}
		if err := migrator.FromSchema(u.Scheme, jsonschema, os.Stdout); err != nil {
			logger.Error("%s", err)
		}
	},
}

func rundiff(cmd *cobra.Command, logger logger.Logger, filename string, drop bool) (*sql.DB, string, []migrator.MigrateChanges, *schema.SchemaJson, *schema.SchemaJson) {
	if !csys.Exists(filename) {
		logger.Fatal("file %s does not exists or is not accessible", filename)
	}
	newSchema, err := migrator.Load(filename)
	if err != nil {
		logger.Fatal("%s", err)
	}
	db, protocol := connectToDB(cmd, logger, newSchema.Database.Url.(string), drop)
	existingSchema, err := migrator.ToSchema(protocol, migrator.ToSchemaArgs{
		Context: context.Background(),
		Logger:  logger,
		DB:      db,
	})
	if err != nil {
		logger.Fatal("%s", err)
	}
	driver := schema.DatabaseDriverType(protocol)
	changes, err := diff.Diff(logger, driver, newSchema, existingSchema)
	if err != nil {
		logger.Fatal("%s", err)
	}
	return db, protocol, changes, existingSchema, newSchema
}

var generateDiffCmd = &cobra.Command{
	Use:   "diff [file]",
	Args:  cobra.ExactArgs(1),
	Short: "Generate diff from a schema",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		db, protocol, changes, _, _ := rundiff(cmd, logger, args[0], false)
		db.Close()
		if len(changes) == 0 {
			fmt.Println("no changes detected")
			return
		}
		format, _ := cmd.Flags().GetString("format")
		driver := schema.DatabaseDriverType(protocol)
		if err := diff.FormatDiff(diff.DiffFormatType(format), driver, changes, os.Stdout); err != nil {
			logger.Fatal("%s", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)
	generateCmd.AddCommand(generateSchemaCmd)
	generateCmd.AddCommand(generateSQLCmd)
	generateCmd.AddCommand(generateDiffCmd)

	generateSchemaCmd.Flags().StringSlice("table", []string{}, "table to filter when generating")
	generateSchemaCmd.Flags().StringP("format", "f", "json", "the output format: json, yaml")

	addUrlFlag(generateSchemaCmd)
	addUrlFlag(generateDiffCmd)

	generateDiffCmd.Flags().StringP("format", "f", "text", "the output format: text, sql")
}
