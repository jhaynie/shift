package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadYAML(t *testing.T) {
	s, err := Load("../testdata/example1.yaml")
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.Equal(t, "1", s.Version)
	assert.Equal(t, "postgres://localhost:5432/db1", s.Database.Url)
	assert.Len(t, s.Tables, 1)
	assert.Equal(t, "table1", s.Tables[0].Name)
	assert.Len(t, s.Tables[0].Columns, 5)

	assert.Equal(t, "id", s.Tables[0].Columns[0].Name)
	assert.NotNil(t, s.Tables[0].Columns[0].PrimaryKey)
	assert.True(t, *s.Tables[0].Columns[0].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[0].Description)
	assert.Equal(t, "This is a description of id", *s.Tables[0].Columns[0].Description)
	assert.NotNil(t, s.Tables[0].Columns[0].Type)
	assert.Equal(t, "int", string(s.Tables[0].Columns[0].Type))

	assert.Equal(t, "count", s.Tables[0].Columns[1].Name)
	assert.Nil(t, s.Tables[0].Columns[1].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[1].Description)
	assert.Equal(t, "This is a description of count", *s.Tables[0].Columns[1].Description)
	assert.NotNil(t, s.Tables[0].Columns[1].Type)
	assert.Equal(t, "int", string(s.Tables[0].Columns[1].Type))
	assert.NotNil(t, s.Tables[0].Columns[1].Default)
	assert.Equal(t, "1", string(*s.Tables[0].Columns[1].Default))

	assert.Equal(t, "name", s.Tables[0].Columns[2].Name)
	assert.Nil(t, s.Tables[0].Columns[2].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[2].Description)
	assert.Equal(t, "This is a description of name", *s.Tables[0].Columns[2].Description)
	assert.NotNil(t, s.Tables[0].Columns[2].Unique)
	assert.True(t, *s.Tables[0].Columns[2].Unique)
	assert.NotNil(t, s.Tables[0].Columns[2].Type)
	assert.Equal(t, "string", string(s.Tables[0].Columns[2].Type))
	assert.Nil(t, s.Tables[0].Columns[2].Default)

	assert.Equal(t, "uuid", s.Tables[0].Columns[3].Name)
	assert.Nil(t, s.Tables[0].Columns[3].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[3].Description)
	assert.Equal(t, "This is a description of uuid", *s.Tables[0].Columns[3].Description)
	assert.Nil(t, s.Tables[0].Columns[3].Unique)
	assert.NotNil(t, s.Tables[0].Columns[3].Type)
	assert.Equal(t, "string", string(s.Tables[0].Columns[3].Type))
	assert.NotNil(t, s.Tables[0].Columns[3].Subtype)
	assert.Equal(t, "uuid", string(*s.Tables[0].Columns[3].Subtype))
	assert.Nil(t, s.Tables[0].Columns[3].Default)

	assert.Equal(t, "ip_address", s.Tables[0].Columns[4].Name)
	assert.Nil(t, s.Tables[0].Columns[4].PrimaryKey)
	assert.NotNil(t, s.Tables[0].Columns[4].Description)
	assert.Equal(t, "This is a description of ip_address", *s.Tables[0].Columns[4].Description)
	assert.Nil(t, s.Tables[0].Columns[4].Unique)
	assert.NotNil(t, s.Tables[0].Columns[4].Type)
	assert.Equal(t, "string", string(s.Tables[0].Columns[4].Type))
	assert.NotNil(t, s.Tables[0].Columns[4].NativeType)
	assert.Equal(t, "cidr", string(*s.Tables[0].Columns[4].NativeType))
	assert.Nil(t, s.Tables[0].Columns[4].Default)
}

func TestLoadJSON(t *testing.T) {
	s1, err := Load("../testdata/example1.yaml")
	assert.NoError(t, err)
	assert.NotNil(t, s1)
	s2, err := Load("../testdata/example1.json")
	assert.NoError(t, err)
	assert.NotNil(t, s2)
	b1, err := json.Marshal(s1)
	assert.NoError(t, err)
	b2, err := json.Marshal(s2)
	assert.NoError(t, err)
	assert.Equal(t, string(b1), string(b2))
}
