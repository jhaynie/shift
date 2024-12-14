package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jhaynie/shift/internal/migrator"
	_ "github.com/jhaynie/shift/internal/migrator/mysql"
	_ "github.com/jhaynie/shift/internal/migrator/postgres"
	"github.com/jhaynie/shift/internal/schema"
	csys "github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "Generate output",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func driverFromURL(urlstr string) (string, string, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", "", err
	}
	switch u.Scheme {
	case "postgres", "postgresql", "pgx":
		return "pgx", u.Scheme, nil
	case "mysql":
		return "mysql", u.Scheme, nil
	case "sqlite":
		return "sqlite", u.Scheme, nil
	case "":
		return "", "", fmt.Errorf("expected --url that provides the database connection url")
	}
	return "", u.Scheme, fmt.Errorf("unsupported protocol: %s", u.Scheme)
}

var generateSchemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Generate schema from an existing database",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		url, _ := cmd.Flags().GetString("url")
		driver, protocol, err := driverFromURL(url)
		if err != nil {
			logger.Fatal("%s", err)
		}
		db, err := sql.Open(driver, url)
		if err != nil {
			logger.Fatal("Unable to connect to database: %v", err)
		}
		defer db.Close()
		jsonschema, err := migrator.ToSchema(protocol, migrator.ToSchemaArgs{
			Context: context.Background(),
			DB:      db,
			Logger:  logger,
		})
		if err != nil {
			logger.Fatal("error generating schema: %s", err)
		}
		buf, err := json.MarshalIndent(jsonschema, " ", "  ")
		if err != nil {
			logger.Fatal("error serializing json: %s", err)
		}
		fmt.Println(string(buf))
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

func init() {
	rootCmd.AddCommand(generateCmd)
	generateCmd.AddCommand(generateSchemaCmd)
	generateCmd.AddCommand(generateSQLCmd)

	generateSchemaCmd.Flags().String("url", os.Getenv("DATABASE_URL"), "the database url")
}
