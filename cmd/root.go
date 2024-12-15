/*
Copyright © 2024 Jeff Haynie

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/jhaynie/shift/internal/migrator"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var tagline = color.BlueString("Super powers for data migrations")

var logo = `
         __    _ ______ 
   _____/ /_  (_) __/ /_
  / ___/ __ \/ / /_/ __/
 (__  ) / / / / __/ /_  
/____/_/ /_/_/_/  \__/  

` + tagline

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "shift",
	Short: tagline,
	Long:  color.HiMagentaString(logo),
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/config/shift/config.yaml)")
}

func newLogger(cmd *cobra.Command) logger.Logger {
	ll, _ := cmd.Flags().GetString("log-level")
	level := logger.LevelInfo
	switch strings.ToLower(ll) {
	case "trace":
		level = logger.LevelTrace
	case "error", "fatal":
		level = logger.LevelError
	case "warn", "warning":
		level = logger.LevelWarn
	case "debug":
		level = logger.LevelDebug
	}
	logger := logger.NewConsoleLogger(level)
	label, _ := cmd.Flags().GetString("log-label")
	if label != "" {
		return logger.WithPrefix("[" + label + "]")
	}
	return logger
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		config := filepath.Join(home, ".config", "shift")
		viper.AddConfigPath(config)
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		if !strings.Contains(err.Error(), "Not Found") {
			fmt.Println("Error reading config file:", viper.ConfigFileUsed(), "error:", err)
			os.Exit(1)
		}
	}
}

func addUrlFlag(cmd *cobra.Command) {
	cmd.Flags().String("url", os.Getenv("DATABASE_URL"), "the database url")
}

func dropDatabase(logger logger.Logger, protocol string, driver string, urlstr string) {
	var currentDB string
	var newurl string
	switch protocol {
	case "postgres":
		u, err := url.Parse(urlstr)
		if err != nil {
			logger.Fatal("%s", err)
		}
		currentDB = u.Path[1:] // get the current database from the path
		u.Path = "/postgres"   // connect without providing a database
		newurl = u.String()
	default:
		logger.Fatal("no drop database provided for %s", protocol)
	}
	db, err := sql.Open(driver, newurl)
	if err != nil {
		logger.Fatal("Unable to connect to database: %v", err)
	}
	ts := time.Now()
	q := fmt.Sprintf("DROP DATABASE IF EXISTS %s", currentDB)
	logger.Trace("sql: %s", q)
	if _, err := db.Exec(q); err != nil {
		logger.Fatal("error dropping database: %s. %s", currentDB, err)
	}
	logger.Info("dropped database %s in %v", currentDB, time.Since(ts))
	ts = time.Now()
	q = fmt.Sprintf("CREATE DATABASE %s", currentDB)
	logger.Trace("sql: %s", q)
	if _, err := db.Exec(q); err != nil {
		logger.Fatal("error creating database: %s. %s", currentDB, err)
	}
	db.Close()
	logger.Info("created database %s in %v", currentDB, time.Since(ts))
}

func connectToDB(cmd *cobra.Command, logger logger.Logger, url string, drop bool) (*sql.DB, string) {
	if url == "" {
		urlstr, _ := cmd.Flags().GetString("url")
		if urlstr == "" {
			logger.Fatal("must provide either --url command line option or set the environment variable DATABASE_URL")
		}
		url = urlstr
	}
	driver, protocol, err := migrator.DriverFromURL(url)
	if err != nil {
		logger.Fatal("%s", err)
	}
	if drop {
		dropDatabase(logger, protocol, driver, url)
	}
	db, err := sql.Open(driver, url)
	if err != nil {
		logger.Fatal("Unable to connect to database: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if _, err := db.QueryContext(ctx, "SELECT 1"); err != nil {
		logger.Fatal("error connecting to %s database ... %s", protocol, err)
	}
	return db, protocol
}

func init() {
	log.SetFlags(0)
	rootCmd.PersistentFlags().String("log-level", "info", "the log level")
	rootCmd.PersistentFlags().String("log-label", "", "a log label to add to the logger")
}
