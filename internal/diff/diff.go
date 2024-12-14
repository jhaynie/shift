package diff

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/fatih/color"
	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/schema"
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

func plural(count int, singular string, plural string) string {
	if count == 0 || count > 1 {
		return plural
	}
	return singular
}

type DiffFormatType string

const (
	FormatText DiffFormatType = "text"
	FormatSQL  DiffFormatType = "sql"
)

func FormatDiff(format DiffFormatType, driver schema.DatabaseDriverType, changes []migrator.MigrateChanges, out io.Writer) {
	switch format {
	case FormatText:
		formatTextDiff(changes, out)
	case FormatSQL:
		formatSQLDiff(driver, changes, out)
	default:
		panic("unsupported diff format: " + string(format))
	}
}

func formatSQLDiff(driver schema.DatabaseDriverType, changes []migrator.MigrateChanges, out io.Writer) {
}

func formatTextDiff(changes []migrator.MigrateChanges, out io.Writer) {
	whiteBold(out, "The following changes need to be applied to bring your database up-to-date:\n\n")
	for _, changeset := range changes {
		switch changeset.Change {
		case migrator.CreateTable:
			green(out, "%s Create ", createSymbol)
			magenta(out, "%s", changeset.Table)
			green(out, " with %d %s:\n", len(changeset.Ref.Columns), plural(len(changeset.Ref.Columns), "column", "columns"))
			formatAddColumnsDiff(changeset, out)
		case migrator.DropTable:
			red(out, "%s Drop ", dropSymbol)
			magenta(out, "%s", changeset.Table)
			red(out, " with %d %s:\n", len(changeset.Ref.Columns), plural(len(changeset.Ref.Columns), "column", "columns"))
			formatDropColumnsDiff(changeset, out)
		case migrator.AlterTable:
			blue(out, "%s Alter ", alterSymbol)
			magenta(out, "%s", changeset.Table)
			if len(changeset.Columns) > 0 {
				blue(out, " with %d %s:\n", len(changeset.Columns), plural(len(changeset.Columns), "column", "columns"))
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
}

func toNativeType(nt *schema.SchemaJsonTablesElemColumnsElemNativeType) string {
	if nt.Postgres != nil {
		return *nt.Postgres
	}
	if nt.Mysql != nil {
		return *nt.Mysql
	}
	if nt.Sqlite != nil {
		return *nt.Sqlite
	}
	return "NULL"
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
		blue(out, "    %s ", alterSymbol)
		whiteBold(out, "%-15s ", column.Name)
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
		} else {
			white(out, "%s\n", changes[0])
			for _, change := range changes[1:] {
				white(out, "%s %s\n", multiPadding, change)
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
