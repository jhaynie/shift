package diff

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/fatih/color"
	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/migrator/types"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/jhaynie/shift/internal/util"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/shopmonkeyus/go-common/logger"
)

func safeNil(val *string) string {
	if val == nil {
		return "NULL"
	}
	return *val
}

func safeBoolNil(val *bool) string {
	if val == nil {
		return "NULL"
	}
	if *val {
		return "true"
	}
	return "false"
}

func pointerChanged[T comparable](a *T, b *T) bool {
	if a == nil && b != nil {
		return true
	}
	if b == nil && a != nil {
		return true
	}
	if a == nil && b == nil {
		return false
	}
	return *a != *b
}

func toDefaultType(val *schema.SchemaJsonTablesElemColumnsElemDefault) *string {
	if val != nil {
		if val.Postgres != nil {
			return val.Postgres
		}
		if val.Mysql != nil {
			return val.Mysql
		}
		if val.Sqlite != nil {
			return val.Sqlite
		}
	}
	return nil
}

func toNativeType(nt *schema.SchemaJsonTablesElemColumnsElemNativeType) string {
	if nt != nil {
		if nt.Postgres != nil {
			return *nt.Postgres
		}
		if nt.Mysql != nil {
			return *nt.Mysql
		}
		if nt.Sqlite != nil {
			return *nt.Sqlite
		}
	}
	return "NULL"
}

func diffColumn(from schema.SchemaJsonTablesElemColumnsElem, to schema.SchemaJsonTablesElemColumnsElem) (*migrator.MigrateColumn, error) {
	var changes []migrator.MigrateColumnChangeTypeType
	if toNativeType(from.NativeType) != toNativeType(to.NativeType) {
		changes = append(changes, migrator.ColumnTypeChanged)
	}
	if pointerChanged(toDefaultType(from.Default), toDefaultType(to.Default)) {
		changes = append(changes, migrator.ColumnDefaultChanged)
	}
	if pointerChanged(from.Description, to.Description) {
		changes = append(changes, migrator.ColumnDescriptionChanged)
	}
	if safeBoolNil(from.Nullable) != safeBoolNil(to.Nullable) && from.Nullable != nil && to.Nullable != nil {
		changes = append(changes, migrator.ColumnNullableChanged)
	}
	if safeBoolNil(from.PrimaryKey) != safeBoolNil(to.PrimaryKey) && from.PrimaryKey != nil && to.PrimaryKey != nil {
		return nil, fmt.Errorf("you cannot change the PRIMARY KEY of a column")
	}
	if safeBoolNil(from.Unique) != safeBoolNil(to.Unique) {
		return nil, fmt.Errorf("you cannot change the UNIQUE consraint of a column")
	}
	if safeBoolNil(from.AutoIncrement) != safeBoolNil(to.AutoIncrement) && from.AutoIncrement != nil && to.AutoIncrement != nil {
		return nil, fmt.Errorf("you cannot change the AUTO INCREMENT of a column")
	}

	if len(changes) > 0 {
		return &migrator.MigrateColumn{
			Change:   migrator.AlterColumn,
			Name:     to.Name,
			Ref:      to,
			Previous: from,
			Changes:  changes,
		}, nil
	}
	return nil, nil
}

func Diff(logger logger.Logger, driver schema.DatabaseDriverType, to *schema.SchemaJson, from *schema.SchemaJson) ([]migrator.MigrateChanges, error) {
	processedTables := make(map[string]bool)
	var res []migrator.MigrateChanges
	var err error

	fromTables := make(map[string]*schema.SchemaJsonTablesElem)
	toTables := make(map[string]*schema.SchemaJsonTablesElem)

	for _, table := range from.Tables {
		fromTables[table.Name] = &table
	}
	for _, table := range to.Tables {
		toTables[table.Name] = &table
	}

	for table, detail := range fromTables {
		if ref, ok := toTables[table]; ok {
			processedTables[table] = true
			logger.Debug("found table %s to already exist, need to validate", table)
			var changes []migrator.MigrateColumn
			var descriptionChange *migrator.MigrateTableDescription
			if pointerChanged(detail.Description, ref.Description) {
				descriptionChange = &migrator.MigrateTableDescription{
					From: detail.Description,
					To:   ref.Description,
				}
			}
			processedColumns := make(map[string]bool)
			for _, toColumn := range ref.Columns {
				processedColumns[toColumn.Name] = true
				var found bool
				var changedRef *migrator.MigrateColumn
				for _, fromColumn := range detail.Columns {
					if fromColumn.Name == toColumn.Name {
						found = true
						changedRef, err = diffColumn(fromColumn, toColumn)
						if err != nil {
							return nil, fmt.Errorf("column %s for table %s encountered an error: %s", table, toColumn.Name, err)
						}
						break
					}
				}
				if !found {
					// need to add
					logger.Debug("column %s not found for %s", toColumn.Name, table)
					changes = append(changes, migrator.MigrateColumn{
						Change: migrator.CreateColumn,
						Name:   toColumn.Name,
						Ref:    toColumn,
					})
				} else if changedRef != nil {
					// updated
					logger.Debug("column %s updated for %s", toColumn.Name, table)
					changes = append(changes, *changedRef)
				}
			}
			for _, fromColumn := range detail.Columns {
				if !processedColumns[fromColumn.Name] {
					logger.Debug("column %s no longer needed for %s", fromColumn.Name, table)
					// need to drop it since its no longer referenced
					changes = append(changes, migrator.MigrateColumn{
						Change: migrator.DropColumn,
						Name:   fromColumn.Name,
						Ref:    fromColumn,
					})
				}
			}
			if len(changes) > 0 {
				res = append(res, migrator.MigrateChanges{
					Change:      migrator.AlterTable,
					Table:       table,
					Columns:     changes,
					Ref:         *detail,
					Description: descriptionChange,
				})
			} else if descriptionChange != nil {
				res = append(res, migrator.MigrateChanges{
					Change:      migrator.AlterTable,
					Table:       table,
					Columns:     nil,
					Ref:         *detail,
					Description: descriptionChange,
				})
			}
		} else {
			logger.Debug("found table %s to no longer exist, need to drop", table)
			res = append(res, migrator.MigrateChanges{
				Change: migrator.DropTable,
				Table:  table,
				Ref:    *detail,
			})
		}
	}

	for table, detail := range toTables {
		if _, ok := processedTables[table]; !ok {
			logger.Debug("found table %s to be missing, need to create", table)
			res = append(res, migrator.MigrateChanges{
				Change: migrator.CreateTable,
				Table:  table,
				Ref:    *detail,
			})
		}
	}
	return res, nil
}

var (
	green     = color.New(color.FgGreen).FprintfFunc()
	red       = color.New(color.FgRed).FprintfFunc()
	blue      = color.New(color.FgBlue).FprintfFunc()
	black     = color.New(color.FgBlack).FprintfFunc()
	white     = color.New(color.FgWhite).FprintfFunc()
	whiteBold = color.New(color.FgWhite, color.Bold).FprintfFunc()
	magenta   = color.New(color.FgMagenta, color.Bold).FprintfFunc()

	createSymbol = "[+]"
	dropSymbol   = "[-]"
	alterSymbol  = "[*]"

	multiPadding = strings.Repeat(" ", 23)
)

type DiffFormatType string

const (
	FormatText DiffFormatType = "text"
	FormatSQL  DiffFormatType = "sql"
)

func FormatDiff(format DiffFormatType, driver schema.DatabaseDriverType, changes []migrator.MigrateChanges, out io.Writer) error {
	switch format {
	case FormatText:
		return formatTextDiff(changes, out)
	case FormatSQL:
		return formatSQLDiff(driver, changes, out)
	default:
		return fmt.Errorf("unsupported diff format: %s", string(format))
	}
}

func formatSQLDiff(driver schema.DatabaseDriverType, changes []migrator.MigrateChanges, out io.Writer) error {
	generator := migrator.GetGenerator(string(driver))
	if generator == nil {
		panic("no generator registered for " + driver)
	}
	for _, changeset := range changes {
		switch changeset.Change {
		case migrator.CreateTable:
			var detail types.TableDetail
			detail.Description = changeset.Ref.Description
			detail.Columns = make([]types.ColumnDetail, len(changeset.Ref.Columns))
			for i, col := range changeset.Ref.Columns {
				val, err := schema.SchemaColumnToColumn(driver, col, i+1, generator.ToNativeType(col))
				if err != nil {
					return fmt.Errorf("error converting column %s for table %s to native type: %s", col.Name, changeset.Table, err)
				}
				detail.Columns[i] = *val
			}
			io.WriteString(out, migrator.GenerateCreateStatement(changeset.Table, detail, generator))
		case migrator.DropTable:
			io.WriteString(out, "DROP TABLE IF EXISTS ")
			io.WriteString(out, generator.QuoteTable(changeset.Table))
			io.WriteString(out, " CASCADE")
			io.WriteString(out, ";\n")
		case migrator.AlterTable:
			if changeset.Description != nil {
				if changeset.Description.To == nil {
					io.WriteString(out, generator.GenerateTableComment(changeset.Table, ""))
				} else {
					io.WriteString(out, generator.GenerateTableComment(changeset.Table, *changeset.Description.To))
				}
				io.WriteString(out, "\n")
			}
			for _, column := range changeset.Columns {
				switch column.Change {
				case migrator.CreateColumn:
					val, err := schema.SchemaColumnToColumn(driver, column.Ref, 0, generator.ToNativeType(column.Ref))
					if err != nil {
						return fmt.Errorf("error converting column %s for table %s to native type: %s", column.Name, changeset.Table, err)
					}
					io.WriteString(out, "ALTER TABLE ")
					io.WriteString(out, generator.QuoteTable(changeset.Table))
					io.WriteString(out, " ")
					io.WriteString(out, "ADD COLUMN ")
					io.WriteString(out, migrator.GenerateColumnStatement(*val, generator, nil))
					io.WriteString(out, ";\n")
				case migrator.DropColumn:
					io.WriteString(out, "ALTER TABLE ")
					io.WriteString(out, generator.QuoteTable(changeset.Table))
					io.WriteString(out, " ")
					io.WriteString(out, "DROP COLUMN ")
					io.WriteString(out, generator.QuoteColumn(column.Name))
					io.WriteString(out, " CASCADE")
					io.WriteString(out, ";\n")
				case migrator.AlterColumn:
					var processed int
					for _, change := range column.Changes {
						if change == migrator.ColumnDescriptionChanged {
							val := column.Ref.Description
							if val == nil || *val == "" {
								io.WriteString(out, generator.GenerateColumnComment(changeset.Table, column.Name, ""))
							} else {
								io.WriteString(out, generator.GenerateColumnComment(changeset.Table, column.Name, *val))
							}
							io.WriteString(out, "\n")
							processed++
						}
					}
					if processed != len(column.Changes) {
						io.WriteString(out, "ALTER TABLE ")
						io.WriteString(out, generator.QuoteTable(changeset.Table))
						io.WriteString(out, " ")
						statements := make([]string, 0)
						for _, change := range column.Changes {
							var sout strings.Builder
							switch change {
							case migrator.ColumnTypeChanged:
								io.WriteString(&sout, "ALTER COLUMN ")
								io.WriteString(&sout, generator.QuoteColumn(column.Name))
								io.WriteString(&sout, " TYPE ")
								io.WriteString(&sout, toNativeType(column.Ref.NativeType))
							case migrator.ColumnDefaultChanged:
								io.WriteString(&sout, "ALTER TABLE ")
								io.WriteString(&sout, generator.QuoteTable(changeset.Table))
								io.WriteString(&sout, " ")
								io.WriteString(&sout, "ALTER COLUMN ")
								io.WriteString(&sout, generator.QuoteColumn(column.Name))
								def := toDefaultType(column.Ref.Default)
								if def == nil || *def == "" {
									io.WriteString(&sout, " DROP DEFAULT")
								} else {
									io.WriteString(&sout, " SET DEFAULT ")
									val, err := schema.SchemaColumnToColumn(driver, column.Ref, 0, generator.ToNativeType(column.Ref))
									if err != nil {
										return fmt.Errorf("error converting column %s for table %s to native type: %s", column.Name, changeset.Table, err)
									}
									io.WriteString(&sout, generator.QuoteDefaultValue(*def, *val))
								}
							case migrator.ColumnNullableChanged:
								io.WriteString(&sout, "ALTER TABLE ")
								io.WriteString(&sout, generator.QuoteTable(changeset.Table))
								io.WriteString(&sout, " ")
								io.WriteString(&sout, "ALTER COLUMN ")
								io.WriteString(&sout, generator.QuoteColumn(column.Name))
								if column.Ref.Nullable == nil || !*column.Ref.Nullable {
									io.WriteString(&sout, " SET NOT NULL")
								} else {
									io.WriteString(&sout, " DROP NOT NULL")
								}
							case migrator.ColumnDescriptionChanged:
							default:
								panic("change " + change + " not handled")
							}
							statements = append(statements, sout.String())
						}
						io.WriteString(out, strings.Join(statements, ", "))
						io.WriteString(out, ";\n")
					}
				}
			}
		}
	}
	return nil
}

func formatTextDiff(changes []migrator.MigrateChanges, out io.Writer) error {
	whiteBold(out, "The following changes need to be applied to bring your database up-to-date:\n\n")
	for _, changeset := range changes {
		switch changeset.Change {
		case migrator.CreateTable:
			green(out, "%s Create ", createSymbol)
			magenta(out, "%s", changeset.Table)
			green(out, " with %d %s:\n", len(changeset.Ref.Columns), util.Plural(len(changeset.Ref.Columns), "column", "columns"))
			formatAddColumnsDiff(changeset, out)
		case migrator.DropTable:
			red(out, "%s Drop ", dropSymbol)
			magenta(out, "%s", changeset.Table)
			red(out, " with %d %s:\n", len(changeset.Ref.Columns), util.Plural(len(changeset.Ref.Columns), "column", "columns"))
			formatDropColumnsDiff(changeset, out)
		case migrator.AlterTable:
			blue(out, "%s Alter ", alterSymbol)
			magenta(out, "%s", changeset.Table)
			if len(changeset.Columns) > 0 {
				blue(out, " with %d %s:\n", len(changeset.Columns), util.Plural(len(changeset.Columns), "column", "columns"))
				formatAlterColumnsDiff(changeset, out)
			} else if changeset.Description != nil {
				blue(out, " with description changed from ")
				io.WriteString(out, color.YellowString(safeNil(changeset.Description.From)))
				io.WriteString(out, color.BlueString(" to "))
				io.WriteString(out, color.YellowString(safeNil(changeset.Description.To)))
			}
		}
		io.WriteString(out, "\n")
	}
	return nil
}

func printColumnChangeRow(column schema.SchemaJsonTablesElemColumnsElem, out io.Writer) {
	whiteBold(out, "%-15s ", column.Name)
	white(out, "%-8s ", column.Type)
	black(out, "%s", toNativeType(column.NativeType))
	io.WriteString(out, "\n")
}

func formatAddColumnsDiff(change migrator.MigrateChanges, out io.Writer) {
	for _, column := range change.Ref.Columns {
		green(out, "    %s ", createSymbol)
		printColumnChangeRow(column, out)
	}
}

func formatDropColumnsDiff(change migrator.MigrateChanges, out io.Writer) {
	for _, column := range change.Ref.Columns {
		red(out, "    %s ", dropSymbol)
		printColumnChangeRow(column, out)
	}
}

func prettyDiff(diffs []diffmatchpatch.Diff) string {
	var buff bytes.Buffer
	for _, diff := range diffs {
		text := diff.Text

		switch diff.Type {
		case diffmatchpatch.DiffInsert:
			_, _ = buff.WriteString("\x1b[32m")
			_, _ = buff.WriteString(text)
			_, _ = buff.WriteString("\x1b[0m")
		case diffmatchpatch.DiffDelete:
			_, _ = buff.WriteString("\x1b[9m\x1b[31m")
			_, _ = buff.WriteString(text)
			_, _ = buff.WriteString("\x1b[0m")
		case diffmatchpatch.DiffEqual:
			_, _ = buff.WriteString("\x1b[33m")
			_, _ = buff.WriteString(text)
			_, _ = buff.WriteString("\x1b[0m")
		}
	}

	return buff.String()
}

func stringDiff(a string, b string) string {
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(a, b, false)
	return prettyDiff(diffs)
}

func formatAlterColumnsDiff(change migrator.MigrateChanges, out io.Writer) {
	for _, column := range change.Columns {
		switch column.Change {
		case migrator.CreateColumn:
			blue(out, "    %s ", createSymbol)
		case migrator.DropColumn:
			blue(out, "    %s ", dropSymbol)
		default:
			blue(out, "    %s ", alterSymbol)
		}
		whiteBold(out, "%-15s ", column.Name)
		switch column.Change {
		case migrator.CreateColumn:
			var val strings.Builder
			white(out, "add column ")
			val.WriteString(color.YellowString(string(column.Ref.Type)))
			val.WriteString(color.BlackString(" (" + toNativeType(column.Ref.NativeType) + ")"))
			blue(out, val.String())
			io.WriteString(out, "\n")
		case migrator.DropColumn:
			white(out, "drop column\n")
		case migrator.AlterColumn:
			changes := make([]string, 0)
			for _, change := range column.Changes {
				var val strings.Builder
				val.WriteString(string(change))
				if change == migrator.ColumnDescriptionChanged {
					val.WriteString(": ")
					val.WriteString(stringDiff(safeNil(column.Previous.Description), safeNil(column.Ref.Description)))
				} else {
					val.WriteString(" from ")
					switch change {
					case migrator.ColumnTypeChanged:
						val.WriteString(color.YellowString(string(column.Previous.Type)))
						val.WriteString(color.BlackString(" (" + toNativeType(column.Previous.NativeType) + ")"))
					case migrator.ColumnDefaultChanged:
						val.WriteString(color.YellowString(safeNil(toDefaultType(column.Previous.Default))))
					case migrator.ColumnDescriptionChanged:
						val.WriteString(color.YellowString(safeNil(column.Previous.Description)))
					case migrator.ColumnNullableChanged:
						val.WriteString(color.YellowString(safeBoolNil(column.Previous.Nullable)))
					default:
						panic("change " + change + " not handled")
					}
					val.WriteString(" to ")
					switch change {
					case migrator.ColumnTypeChanged:
						val.WriteString(color.YellowString(string(column.Ref.Type)))
						val.WriteString(color.BlackString(" (" + toNativeType(column.Ref.NativeType) + ")"))
					case migrator.ColumnDefaultChanged:
						val.WriteString(color.YellowString(safeNil(toDefaultType(column.Ref.Default))))
					case migrator.ColumnDescriptionChanged:
						val.WriteString(color.YellowString(safeNil(column.Ref.Description)))
					case migrator.ColumnNullableChanged:
						val.WriteString(color.YellowString(safeBoolNil(column.Ref.Nullable)))
					default:
						panic("change " + change + " not handled")
					}
				}
				changes = append(changes, val.String())
			}
			if len(changes) == 1 {
				white(out, "%s\n", changes[0])
			} else if len(changes) > 0 {
				white(out, "%s\n", changes[0])
				if len(changes) > 1 {
					for _, change := range changes[1:] {
						white(out, "%s %s\n", multiPadding, change)
					}
				}
			} else {
				io.WriteString(out, color.RedString("missing changes\n"))
			}
		}
	}
	if change.Description != nil {
		io.WriteString(out, "\n")
		io.WriteString(out, color.BlueString("    table description changed from "))
		io.WriteString(out, stringDiff(safeNil(change.Description.From), safeNil(change.Description.To)))
		io.WriteString(out, "\n")
	}
}
