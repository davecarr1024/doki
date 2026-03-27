package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCatalog_CreateAndGetTable(t *testing.T) {
	c := NewInMemoryCatalog()

	def := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: TypeInt, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: TypeText, PrimaryKey: false, NotNull: false},
		},
		PrimaryKey: "id",
		ShardID:    "shard-1",
	}

	err := c.CreateTable(def)
	require.NoError(t, err)

	got, err := c.GetTable("users")
	require.NoError(t, err)
	assert.Equal(t, def, got)
}

func TestCatalog_GetTable_NotFound(t *testing.T) {
	c := NewInMemoryCatalog()
	_, err := c.GetTable("nonexistent")
	assert.Error(t, err)
}

func TestCatalog_CreateTable_Duplicate(t *testing.T) {
	c := NewInMemoryCatalog()

	def := TableDef{Name: "t", Columns: []ColumnDef{{Name: "id", Type: TypeInt}}}
	require.NoError(t, c.CreateTable(def))

	err := c.CreateTable(def)
	assert.Error(t, err)
}

func TestCatalog_ListTables(t *testing.T) {
	c := NewInMemoryCatalog()

	tables := c.ListTables()
	assert.Empty(t, tables)

	def1 := TableDef{Name: "a", Columns: []ColumnDef{{Name: "id", Type: TypeInt}}}
	def2 := TableDef{Name: "b", Columns: []ColumnDef{{Name: "id", Type: TypeInt}}}

	require.NoError(t, c.CreateTable(def1))
	require.NoError(t, c.CreateTable(def2))

	tables = c.ListTables()
	assert.Len(t, tables, 2)

	names := make(map[string]bool)
	for _, td := range tables {
		names[td.Name] = true
	}
	assert.True(t, names["a"])
	assert.True(t, names["b"])
}

func TestRowKey(t *testing.T) {
	assert.Equal(t, "users/42", RowKey("users", "42"))
	assert.Equal(t, "orders/abc-123", RowKey("orders", "abc-123"))
}

func TestEncodeDecodeRow(t *testing.T) {
	row := map[string]any{
		"id":     float64(1), // JSON numbers decode as float64
		"name":   "alice",
		"active": true,
	}

	data, err := EncodeRow(row)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded, err := DecodeRow(data)
	require.NoError(t, err)
	assert.Equal(t, row, decoded)
}

func TestEncodeRow_Error(t *testing.T) {
	// channels cannot be marshaled to JSON
	row := map[string]any{
		"bad": make(chan int),
	}
	_, err := EncodeRow(row)
	assert.Error(t, err)
}

func TestDecodeRow_Error(t *testing.T) {
	_, err := DecodeRow([]byte("not valid json"))
	assert.Error(t, err)
}

func TestColumnType_Values(t *testing.T) {
	assert.Equal(t, ColumnType(0), TypeInt)
	assert.Equal(t, ColumnType(1), TypeText)
	assert.Equal(t, ColumnType(2), TypeBool)
}
