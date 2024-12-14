package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"gopkg.in/yaml.v3"
)

const (
	DefaultVersion = "1"
	DefaultSchema  = "https://raw.githubusercontent.com/jhaynie/shift/refs/heads/main/schema.json"
)

var nameRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func validateName(name string) bool {
	return nameRegex.MatchString(name)
}

// Load reads a schema from a file in either YAML or JSON format.
func Load(filename string) (*SchemaJson, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var schema SchemaJson
	switch filepath.Ext(filename) {
	case ".yaml", ".yml":
		dec := yaml.NewDecoder(f)
		if err := dec.Decode(&schema); err != nil {
			return nil, err
		}
	case ".json":
		dec := json.NewDecoder(f)
		if err := dec.Decode(&schema); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported file extension: %s. should be either .json or .yaml", filepath.Ext(filename))
	}
	for _, table := range schema.Tables {
		if !validateName(table.Name) {
			return nil, fmt.Errorf("table `%s` has an invalid name", table.Name)
		}
		for _, col := range table.Columns {
			if !validateName(col.Name) {
				return nil, fmt.Errorf("column `%s` in table `%s` has an invalid name", col.Name, table.Name)
			}
		}
	}
	return &schema, nil
}

type SchemaJsonForOutput struct {
	// The URL to the Shift schema.
	Schema string `json:"$schema" yaml:"-" mapstructure:"-"`

	// The version of the Shift configuration file.
	Version string `json:"version" yaml:"version" mapstructure:"version"`

	// The database configuration for the migration to use.
	Database SchemaJsonDatabase `json:"database" yaml:"database" mapstructure:"database"`

	// The tables to manage in the migration.
	Tables []SchemaJsonTablesElem `json:"tables" yaml:"tables" mapstructure:"tables"`
}
