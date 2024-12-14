package diff

import (
	"io"

	"github.com/fatih/color"
	"github.com/jhaynie/shift/internal/migrator"
	"github.com/jhaynie/shift/internal/schema"
	"github.com/jhaynie/shift/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

func diffColumn(from schema.SchemaJsonTablesElemColumnsElem, to schema.SchemaJsonTablesElemColumnsElem) *migrator.MigrateColumn {
	var changeType *string // what is changing, the type, etc
	var changeFrom *string // what its going from
	var changeTo *string   /// what its going to
	if toNativeType(from.NativeType) != toNativeType(to.NativeType) {
		changeType = util.Ptr("type")
		changeFrom = util.Ptr(toNativeType(from.NativeType))
		changeTo = util.Ptr(toNativeType(to.NativeType))
	}
	if changeType != nil {
		return &migrator.MigrateColumn{
			Change:     migrator.AlterColumn,
			Name:       to.Name,
			Ref:        to,
			Previous:   from,
			ChangeType: *changeType,
			ChangeFrom: *changeFrom,
			ChangeTo:   *changeTo,
		}
	}
	return nil
}

func Diff(logger logger.Logger, driver schema.DatabaseDriverType, to *schema.SchemaJson, from *schema.SchemaJson) ([]migrator.MigrateChanges, error) {
	processedTables := make(map[string]bool)
	var res []migrator.MigrateChanges

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
			processedColumns := make(map[string]bool)
			for _, toColumn := range ref.Columns {
				processedColumns[toColumn.Name] = true
				var found bool
				var changedRef *migrator.MigrateColumn
				for _, fromColumn := range detail.Columns {
					if fromColumn.Name == toColumn.Name {
						found = true
						changedRef = diffColumn(fromColumn, toColumn)
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
					Change:  migrator.AlterTable,
					Table:   table,
					Columns: changes,
					Ref:     *detail,
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
)

func FormatDiff(changes []migrator.MigrateChanges, out io.Writer) {
	whiteBold(out, "The following changes need to be applied to bring your database up-to-date:\n\n")
	for _, changeset := range changes {
		switch changeset.Change {
		case migrator.CreateTable:
			green(out, "%s Create ", createSymbol)
			magenta(out, "%s", changeset.Table)
			green(out, " with %d columns:\n", len(changeset.Ref.Columns))
			formatAddColumnsDiff(changeset, out)
		case migrator.DropTable:
			red(out, "%s Drop ", dropSymbol)
			magenta(out, "%s", changeset.Table)
			red(out, " with %d columns:\n", len(changeset.Ref.Columns))
			formatDropColumnsDiff(changeset, out)
		case migrator.AlterTable:
			blue(out, "%s Alter ", alterSymbol)
			magenta(out, "%s", changeset.Table)
			blue(out, " with %d column changes:\n", len(changeset.Columns))
			formatAlterColumnsDiff(changeset, out)
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
	return "<nil>"
}

func printColumnChangeRow(column schema.SchemaJsonTablesElemColumnsElem, out io.Writer) {
	whiteBold(out, "%-20s ", column.Name)
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

func formatAlterColumnsDiff(change migrator.MigrateChanges, out io.Writer) {
	for _, column := range change.Columns {
		blue(out, "    %s %s %s %s -> %s\n", alterSymbol, column.Name, column.ChangeType, column.ChangeFrom, column.ChangeTo)
		// printColumnChangeRow(column, out)
	}
}
