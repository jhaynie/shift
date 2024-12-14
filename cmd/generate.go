package cmd

import (
	"context"
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

var generateDiffCmd = &cobra.Command{
	Use:   "diff [file]",
	Args:  cobra.ExactArgs(1),
	Short: "Generate diff from a schema",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		file := args[0]
		if !csys.Exists(file) {
			logger.Fatal("file %s does not exists or is not accessible", file)
		}
		newSchema, err := migrator.Load(file)
		if err != nil {
			logger.Fatal("%s", err)
		}
		db, protocol := connectToDB(cmd, logger, newSchema.Database.Url.(string), false)
		defer db.Close()
		existingSchema, err := migrator.ToSchema(protocol, migrator.ToSchemaArgs{
			Context: context.Background(),
			Logger:  logger,
			DB:      db,
		})
		if err != nil {
			logger.Fatal("%s", err)
		}
		changes, err := diff.Diff(logger, schema.DatabaseDriverType(protocol), newSchema, existingSchema)
		if err != nil {
			logger.Fatal("%s", err)
		}
		if len(changes) == 0 {
			fmt.Println("no changes detected")
			return
		}
		diff.FormatDiff(changes, os.Stdout)
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
}
