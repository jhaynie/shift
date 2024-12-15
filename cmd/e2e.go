//go:build e2e
// +build e2e

package cmd

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/jhaynie/shift/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

func dockerDown(logger logger.Logger, docker string, cwd string) {
	c := exec.Command(docker, "compose", "down", "--timeout=10")
	c.Dir = cwd
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Run()
	logger.Debug("docker compose down")
}

type dockerStatus struct {
	ID    string
	State string
	Names string
}

func waitForReady(logger logger.Logger, docker string, cwd string) {
	started := time.Now()
	for time.Since(started) < time.Minute {
		c := exec.Command(docker, "ps", "--format=json")
		c.Dir = cwd
		buf, err := c.CombinedOutput()
		if err != nil {
			logger.Fatal("error running docker ps: %s", err)
		}
		dec := json.NewDecoder(bytes.NewReader(buf))
		var count, ready int
		for dec.More() {
			var status dockerStatus
			if err := dec.Decode(&status); err != nil {
				logger.Fatal("error parsing docker ps json: %s", err)
			}
			logger.Trace("id=%s,name=%s,state=%s", status.ID, status.Names, status.State)
			if strings.HasPrefix(status.Names, "shift-") {
				count++
				if status.State == "running" {
					ready++
				}
			}
		}
		logger.Trace("count=%d, ready=%d", count, ready)
		if count > 0 && count == ready {
			if os.Getenv("CI") == "true" {
				logger.Info("waiting for 5 seconds for containers to be ready...")
				time.Sleep(time.Second * 5)
			}
			c = exec.Command(docker, "logs", "shift-postgres-1")
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			c.Run()
			return
		}
		time.Sleep(time.Second)
	}
}

func runDiff(logger logger.Logger, filename string, label string, level string) {
	bin, err := os.Executable()
	if err != nil {
		bin = os.Args[0]
	}
	logger.Debug("running diff: %s", filename)
	c := exec.Command(bin, "generate", "diff", filename, "--log-label", label, "--format=sql", "--log-level", level)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Env = append(os.Environ(), "DB_PASSWORD=shift1234%23")
	if err := c.Run(); err != nil {
		logger.Fatal("error running %s. %s", filename, err)
	}
}

func runForceMigration(logger logger.Logger, filename string, label string, level string) {
	runDiff(logger, filename, label, level)
	bin, err := os.Executable()
	if err != nil {
		bin = os.Args[0]
	}
	logger.Debug("running migration: %s", filename)
	c := exec.Command(bin, "migrate", filename, "--confirm=false", "--log-label", label, "--log-level", level)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	c.Env = append(os.Environ(), "DB_PASSWORD=shift1234%23")
	if err := c.Run(); err != nil {
		logger.Fatal("error running %s. %s", filename, err)
	}
}

var e2eCmd = &cobra.Command{
	Use:   "e2e",
	Short: "Run the e2e test suite",
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		docker, err := exec.LookPath("docker")
		if err != nil {
			logger.Fatal("error finding docker: %s", err)
		}
		shutdown, _ := cmd.Flags().GetBool("shutdown")
		level, _ := cmd.Flags().GetString("log-level")
		if level == "" {
			level = "info"
		}
		cwd, _ := os.Getwd()
		c := exec.Command(docker, "compose", "up", "-d")
		c.Dir = cwd
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		c.Stdin = os.Stdin
		if err := c.Start(); err != nil {
			logger.Fatal("error starting docker compose: %s", err)
		}
		waitForReady(logger, docker, cwd)
		var stopped bool
		defer func() {
			if shutdown && !stopped {
				dockerDown(logger, docker, cwd)
			}
		}()
		basedir := filepath.Join(cwd, "e2e")
		files, err := sys.ListDir(basedir)
		if err != nil {
			logger.Error("error loading driver files: %s", err)
			return
		}
		drivers, _ := cmd.Flags().GetStringSlice("driver")
		for _, filename := range files {
			driver := filepath.Base(filepath.Dir(filename))
			if len(drivers) > 0 && !util.Contains(drivers, driver) {
				continue
			}
			label, _ := filepath.Rel(basedir, filename)
			runForceMigration(logger, filename, strings.Replace(label, ".yml", "", 1), level)
		}
		if shutdown {
			dockerDown(logger, docker, cwd)
			stopped = true
		}
	},
}

func init() {
	rootCmd.AddCommand(e2eCmd)
	e2eCmd.Flags().StringSlice("driver", []string{}, "filter tests by driver name")
	e2eCmd.Flags().Bool("shutdown", true, "if the containers should be shutdown upon completion")
}
