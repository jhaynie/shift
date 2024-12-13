package migrator

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jhaynie/shift/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/stretchr/testify/assert"
)

func TestGenerateDefaultNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{}))
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateDefaultNoRowsError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnError(sql.ErrNoRows)
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateWithTableCatalog(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = 'catalog' \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{}))
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db, WithTableCatalog("catalog"))
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateWithTableCatalogAsFunctions(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = catalog\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{}))
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db, WithTableCatalog("catalog()"))
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateWithTableType(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'type' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{}))
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db, WithTableType("type"))
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateWithTableSchemaExcludes(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema','table'\\) AND table_catalog = current_database\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{}))
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db, WithTableSchemaExcludes([]string{"table"}))
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateSingleTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "character_maximum_length", "numeric_precision", "udt_name"}).AddRow("table", "column", int64(1), nil, "NO", "text", nil, nil, "text"))
	mock.ExpectQuery("SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints WHERE table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) AND constraint_type != 'CHECK' ORDER BY table_name").WithoutArgs().WillReturnError(sql.ErrNoRows)
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotEmpty(t, res)
	assert.NotEmpty(t, res["table"])
	assert.NotEmpty(t, res["table"].Columns)
	assert.Empty(t, res["table"].Constraints)
	assert.Equal(t, "column", res["table"].Columns[0].Name)
	assert.Equal(t, "text", res["table"].Columns[0].DataType)
	assert.Equal(t, "text", res["table"].Columns[0].UDTName)
	assert.Nil(t, res["table"].Columns[0].Default)
	assert.Nil(t, res["table"].Columns[0].MaxLength)
	assert.Nil(t, res["table"].Columns[0].NumericPrecision)
	assert.Equal(t, int64(1), res["table"].Columns[0].Ordinal)
	assert.False(t, res["table"].Columns[0].IsNullable)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateSingleTableWithNullable(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "character_maximum_length", "numeric_precision", "udt_name"}).AddRow("table", "column", int64(1), nil, "YES", "text", nil, nil, "text"))
	mock.ExpectQuery("SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints WHERE table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) AND constraint_type != 'CHECK' ORDER BY table_name").WithoutArgs().WillReturnError(sql.ErrNoRows)
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotEmpty(t, res)
	assert.NotEmpty(t, res["table"])
	assert.NotEmpty(t, res["table"].Columns)
	assert.Empty(t, res["table"].Constraints)
	assert.Equal(t, "column", res["table"].Columns[0].Name)
	assert.Equal(t, "text", res["table"].Columns[0].DataType)
	assert.Equal(t, "text", res["table"].Columns[0].UDTName)
	assert.Nil(t, res["table"].Columns[0].Default)
	assert.Nil(t, res["table"].Columns[0].MaxLength)
	assert.Nil(t, res["table"].Columns[0].NumericPrecision)
	assert.Equal(t, int64(1), res["table"].Columns[0].Ordinal)
	assert.True(t, res["table"].Columns[0].IsNullable)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateSingleTableWithDefault(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "character_maximum_length", "numeric_precision", "udt_name"}).AddRow("table", "column", int64(1), util.Ptr("default"), "YES", "text", nil, nil, "text"))
	mock.ExpectQuery("SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints WHERE table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) AND constraint_type != 'CHECK' ORDER BY table_name").WithoutArgs().WillReturnError(sql.ErrNoRows)
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotEmpty(t, res)
	assert.NotEmpty(t, res["table"])
	assert.NotEmpty(t, res["table"].Columns)
	assert.Empty(t, res["table"].Constraints)
	assert.Equal(t, "column", res["table"].Columns[0].Name)
	assert.Equal(t, "text", res["table"].Columns[0].DataType)
	assert.Equal(t, "text", res["table"].Columns[0].UDTName)
	assert.NotNil(t, res["table"].Columns[0].Default)
	assert.Equal(t, "default", *res["table"].Columns[0].Default)
	assert.Nil(t, res["table"].Columns[0].MaxLength)
	assert.Nil(t, res["table"].Columns[0].NumericPrecision)
	assert.Equal(t, int64(1), res["table"].Columns[0].Ordinal)
	assert.True(t, res["table"].Columns[0].IsNullable)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGenerateSingleTableWithPrimaryKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery("SELECT table_name, column_name, ordinal_position, column_default, is_nullable, data_type, character_maximum_length, numeric_precision, udt_name FROM information_schema.columns WHERE table_name IN \\( SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) \\) ORDER BY table_name, ordinal_position").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{"table_name", "column_name", "ordinal_position", "column_default", "is_nullable", "data_type", "character_maximum_length", "numeric_precision", "udt_name"}).AddRow("table", "column", int64(1), util.Ptr("default"), "YES", "text", nil, nil, "text"))
	mock.ExpectQuery("SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints WHERE table_schema NOT IN \\('pg_catalog','information_schema'\\) AND table_catalog = current_database\\(\\) AND constraint_type != 'CHECK' ORDER BY table_name").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{"constraint_name", "table_name", "constraint_type"}).AddRow("table_pk", "table", "PRIMARY KEY"))
	res, err := GenerateInfoTables(context.Background(), logger.NewTestLogger(), db)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotEmpty(t, res)
	assert.NotEmpty(t, res["table"])
	assert.NotEmpty(t, res["table"].Columns)
	assert.NotEmpty(t, res["table"].Constraints)
	assert.Equal(t, "column", res["table"].Columns[0].Name)
	assert.Equal(t, "text", res["table"].Columns[0].DataType)
	assert.Equal(t, "text", res["table"].Columns[0].UDTName)
	assert.NotNil(t, res["table"].Columns[0].Default)
	assert.Equal(t, "default", *res["table"].Columns[0].Default)
	assert.Nil(t, res["table"].Columns[0].MaxLength)
	assert.Nil(t, res["table"].Columns[0].NumericPrecision)
	assert.Equal(t, int64(1), res["table"].Columns[0].Ordinal)
	assert.True(t, res["table"].Columns[0].IsNullable)
	assert.Len(t, res["table"].Constraints, 1)
	assert.Equal(t, "table_pk", res["table"].Constraints[0].Name)
	assert.Equal(t, "PRIMARY KEY", res["table"].Constraints[0].Type)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
