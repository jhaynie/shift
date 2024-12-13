package types

type TableDetail struct {
	Columns     []ColumnDetail
	Constraints []ConstraintDetail
	Description *string
}

type ColumnDetail struct {
	Name               string
	Ordinal            int64
	Default            *string
	DataType           string
	UDTName            string
	MaxLength          *int64
	NumericPrecision   *int64
	Description        *string
	IsNullable         bool
	IsPrimaryKey       bool
	IsUnique           bool
	IsAutoIncrementing bool
}

type ConstraintDetail struct {
	Name string
	Type string
}
