package sql

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned by KV.Get when a key does not exist.
var ErrNotFound = errors.New("not found")

// KV is the interface the executor uses to access the storage layer.
type KV interface {
	Get(key string) (string, error)
	Put(key, value string) error
	Delete(key string) error
	Scan(prefix string) ([]KVEntry, error)
}

// KVEntry is a key-value pair returned by KV.Scan.
type KVEntry struct {
	Key   string
	Value string
}

// ResultSet holds the result of a SQL execution.
type ResultSet struct {
	Columns      []string
	Rows         []map[string]any
	RowsAffected int
}

// Execute runs a PhysicalPlan against the given KV store and catalog, returning a ResultSet.
func Execute(plan PhysicalPlan, kv KV, cat Catalog) (*ResultSet, error) {
	switch p := plan.(type) {
	case *CreateTablePlan:
		return executeCreateTable(p, cat)
	case *InsertPlan:
		return executeInsert(p, kv)
	case *PointGet:
		return executePointGet(p, kv)
	case *TableScan:
		return executeTableScan(p, kv)
	case *PointUpdate:
		return executePointUpdate(p, kv)
	case *PointDelete:
		return executePointDelete(p, kv)
	default:
		return nil, fmt.Errorf("unknown plan type %T", plan)
	}
}

func executeCreateTable(p *CreateTablePlan, cat Catalog) (*ResultSet, error) {
	if err := cat.CreateTable(p.Def); err != nil {
		return nil, fmt.Errorf("CREATE TABLE: %w", err)
	}
	return &ResultSet{}, nil
}

func executeInsert(p *InsertPlan, kv KV) (*ResultSet, error) {
	data, err := EncodeRow(p.Row)
	if err != nil {
		return nil, fmt.Errorf("INSERT: encode row: %w", err)
	}
	if err := kv.Put(p.Key, string(data)); err != nil {
		return nil, fmt.Errorf("INSERT: kv put: %w", err)
	}
	return &ResultSet{RowsAffected: 1}, nil
}

func executePointGet(p *PointGet, kv KV) (*ResultSet, error) {
	val, err := kv.Get(p.Key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return &ResultSet{Columns: columnNames(p.Table, p.Columns)}, nil
		}
		return nil, fmt.Errorf("SELECT: kv get: %w", err)
	}

	row, err := DecodeRow([]byte(val))
	if err != nil {
		return nil, fmt.Errorf("SELECT: decode row: %w", err)
	}

	cols := resolveColumns(p.Table, p.Columns)
	projected := projectRow(row, cols)

	return &ResultSet{
		Columns: columnNamesFromDefs(cols),
		Rows:    []map[string]any{projected},
	}, nil
}

func executeTableScan(p *TableScan, kv KV) (*ResultSet, error) {
	entries, err := kv.Scan(p.Prefix)
	if err != nil {
		return nil, fmt.Errorf("SELECT: kv scan: %w", err)
	}

	cols := resolveColumns(p.Table, p.Columns)
	colNames := columnNamesFromDefs(cols)

	var rows []map[string]any
	for _, entry := range entries {
		row, err := DecodeRow([]byte(entry.Value))
		if err != nil {
			return nil, fmt.Errorf("SELECT: decode row for key %q: %w", entry.Key, err)
		}
		rows = append(rows, projectRow(row, cols))
	}

	return &ResultSet{
		Columns: colNames,
		Rows:    rows,
	}, nil
}

func executePointUpdate(p *PointUpdate, kv KV) (*ResultSet, error) {
	val, err := kv.Get(p.Key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return &ResultSet{RowsAffected: 0}, nil
		}
		return nil, fmt.Errorf("UPDATE: kv get: %w", err)
	}

	row, err := DecodeRow([]byte(val))
	if err != nil {
		return nil, fmt.Errorf("UPDATE: decode row: %w", err)
	}

	// Apply assignments
	for _, a := range p.Assignments {
		lit, ok := a.Value.Expr.(*Literal)
		if !ok {
			return nil, fmt.Errorf("UPDATE: non-literal value for column %q", a.Column.Name)
		}
		row[a.Column.Name] = lit.Value
	}

	data, err := EncodeRow(row)
	if err != nil {
		return nil, fmt.Errorf("UPDATE: encode row: %w", err)
	}
	if err := kv.Put(p.Key, string(data)); err != nil {
		return nil, fmt.Errorf("UPDATE: kv put: %w", err)
	}

	return &ResultSet{RowsAffected: 1}, nil
}

func executePointDelete(p *PointDelete, kv KV) (*ResultSet, error) {
	if err := kv.Delete(p.Key); err != nil {
		return nil, fmt.Errorf("DELETE: kv delete: %w", err)
	}
	return &ResultSet{RowsAffected: 1}, nil
}

// resolveColumns returns the columns to project. If cols is nil, returns all table columns.
func resolveColumns(table TableDef, cols []ColumnDef) []ColumnDef {
	if cols == nil {
		return table.Columns
	}
	return cols
}

// columnNames returns column name strings for a projection (nil cols = all table cols).
func columnNames(table TableDef, cols []ColumnDef) []string {
	return columnNamesFromDefs(resolveColumns(table, cols))
}

// columnNamesFromDefs extracts column names from a slice of ColumnDef.
func columnNamesFromDefs(cols []ColumnDef) []string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// projectRow returns only the fields named in cols from row.
func projectRow(row map[string]any, cols []ColumnDef) map[string]any {
	result := make(map[string]any, len(cols))
	for _, col := range cols {
		result[col.Name] = row[col.Name]
	}
	return result
}
