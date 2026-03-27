package sql

import (
	"encoding/json"
	"fmt"
	"sync"
)

// ColumnType represents the SQL type of a column.
type ColumnType int

const (
	TypeInt  ColumnType = iota
	TypeText
	TypeBool
)

// ColumnDef describes a single column in a table.
type ColumnDef struct {
	Name       string
	Type       ColumnType
	PrimaryKey bool
	NotNull    bool
}

// TableDef describes a table's schema.
type TableDef struct {
	Name       string
	Columns    []ColumnDef
	PrimaryKey string // column name of the primary key
	ShardID    string // which shard stores rows for this table
}

// Catalog is the interface for schema management.
type Catalog interface {
	CreateTable(def TableDef) error
	GetTable(name string) (TableDef, error)
	ListTables() []TableDef
}

// inMemoryCatalog is a thread-safe in-memory Catalog implementation.
type inMemoryCatalog struct {
	mu     sync.RWMutex
	tables map[string]TableDef
}

// NewInMemoryCatalog returns a new in-memory Catalog.
func NewInMemoryCatalog() Catalog {
	return &inMemoryCatalog{
		tables: make(map[string]TableDef),
	}
}

// CreateTable adds a new table definition to the catalog.
// Returns an error if a table with the same name already exists.
func (c *inMemoryCatalog) CreateTable(def TableDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[def.Name]; exists {
		return fmt.Errorf("table %q already exists", def.Name)
	}
	c.tables[def.Name] = def
	return nil
}

// GetTable retrieves a table definition by name.
// Returns an error if the table does not exist.
func (c *inMemoryCatalog) GetTable(name string) (TableDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	def, ok := c.tables[name]
	if !ok {
		return TableDef{}, fmt.Errorf("table %q not found", name)
	}
	return def, nil
}

// ListTables returns all table definitions in the catalog.
func (c *inMemoryCatalog) ListTables() []TableDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tables := make([]TableDef, 0, len(c.tables))
	for _, def := range c.tables {
		tables = append(tables, def)
	}
	return tables
}

// RowKey returns the KV key for a row given a table name and primary key value.
func RowKey(table, pkValue string) string {
	return table + "/" + pkValue
}

// EncodeRow encodes a row (map of column name to value) to JSON bytes.
func EncodeRow(row map[string]any) ([]byte, error) {
	data, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("encode row: %w", err)
	}
	return data, nil
}

// DecodeRow decodes JSON bytes into a row map.
func DecodeRow(data []byte) (map[string]any, error) {
	var row map[string]any
	if err := json.Unmarshal(data, &row); err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}
	return row, nil
}
