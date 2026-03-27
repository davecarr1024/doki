package sql

import (
	"fmt"
	"strconv"
)

// PhysicalPlan is the interface for all executable plans.
type PhysicalPlan interface{ physicalPlanNode() }

// PointGet fetches a single row by its full KV key.
type PointGet struct {
	Key     string
	Table   TableDef
	Columns []ColumnDef // nil = all
}

func (p *PointGet) physicalPlanNode() {}

// PointPut writes a single KV key.
type PointPut struct {
	Key   string
	Value map[string]any
	Table TableDef
}

func (p *PointPut) physicalPlanNode() {}

// PointDelete deletes a single KV key.
type PointDelete struct {
	Key   string
	Table TableDef
}

func (p *PointDelete) physicalPlanNode() {}

// PointUpdate reads then writes a single KV key.
type PointUpdate struct {
	Key         string
	Table       TableDef
	Assignments []ResolvedAssignment
}

func (p *PointUpdate) physicalPlanNode() {}

// TableScan is a full table prefix scan (no WHERE or non-PK WHERE).
type TableScan struct {
	Prefix  string
	Table   TableDef
	Columns []ColumnDef // nil = all
}

func (p *TableScan) physicalPlanNode() {}

// CreateTablePlan is a DDL plan.
type CreateTablePlan struct {
	Def TableDef
}

func (p *CreateTablePlan) physicalPlanNode() {}

// InsertPlan inserts a new row.
type InsertPlan struct {
	Key   string
	Row   map[string]any
	Table TableDef
}

func (p *InsertPlan) physicalPlanNode() {}

// Plan converts a ResolvedStatement into a PhysicalPlan.
func Plan(rs ResolvedStatement) (PhysicalPlan, error) {
	switch s := rs.(type) {
	case *ResolvedCreateTable:
		return &CreateTablePlan{Def: s.Def}, nil
	case *ResolvedInsert:
		return planInsert(s)
	case *ResolvedSelect:
		return planSelect(s)
	case *ResolvedUpdate:
		return planUpdate(s)
	case *ResolvedDelete:
		return planDelete(s)
	default:
		return nil, fmt.Errorf("unknown resolved statement type %T", rs)
	}
}

func planInsert(s *ResolvedInsert) (PhysicalPlan, error) {
	// Find PK column and its value
	pkValue, err := findPKValue(s.Table, s.Cols, s.Values)
	if err != nil {
		return nil, fmt.Errorf("plan INSERT: %w", err)
	}

	pkStr, err := evalLiteralToString(pkValue)
	if err != nil {
		return nil, fmt.Errorf("plan INSERT: pk value: %w", err)
	}

	// Build row map
	row := make(map[string]any, len(s.Cols))
	for i, col := range s.Cols {
		lit, ok := s.Values[i].Expr.(*Literal)
		if !ok {
			return nil, fmt.Errorf("plan INSERT: non-literal value for column %q", col.Name)
		}
		row[col.Name] = lit.Value
	}

	key := RowKey(s.Table.Name, pkStr)
	return &InsertPlan{Key: key, Row: row, Table: s.Table}, nil
}

func planSelect(s *ResolvedSelect) (PhysicalPlan, error) {
	if s.Where != nil && s.Where.Op == "=" {
		pkStr, err := evalLiteralToString(s.Where.Value)
		if err != nil {
			return nil, fmt.Errorf("plan SELECT: WHERE value: %w", err)
		}
		key := RowKey(s.Table.Name, pkStr)
		return &PointGet{Key: key, Table: s.Table, Columns: s.Columns}, nil
	}
	// Full table scan
	prefix := s.Table.Name + "/"
	return &TableScan{Prefix: prefix, Table: s.Table, Columns: s.Columns}, nil
}

func planUpdate(s *ResolvedUpdate) (PhysicalPlan, error) {
	if s.Where == nil {
		return nil, fmt.Errorf("plan UPDATE: WHERE clause required")
	}
	if s.Where.Op != "=" {
		return nil, fmt.Errorf("plan UPDATE: only = operator supported in WHERE")
	}
	pkStr, err := evalLiteralToString(s.Where.Value)
	if err != nil {
		return nil, fmt.Errorf("plan UPDATE: WHERE value: %w", err)
	}
	key := RowKey(s.Table.Name, pkStr)
	return &PointUpdate{Key: key, Table: s.Table, Assignments: s.Assignments}, nil
}

func planDelete(s *ResolvedDelete) (PhysicalPlan, error) {
	if s.Where == nil {
		return nil, fmt.Errorf("plan DELETE: WHERE clause required")
	}
	if s.Where.Op != "=" {
		return nil, fmt.Errorf("plan DELETE: only = operator supported in WHERE")
	}
	pkStr, err := evalLiteralToString(s.Where.Value)
	if err != nil {
		return nil, fmt.Errorf("plan DELETE: WHERE value: %w", err)
	}
	key := RowKey(s.Table.Name, pkStr)
	return &PointDelete{Key: key, Table: s.Table}, nil
}

// evalLiteralToString converts a TypedExpr holding a literal value to its string representation
// for use as a KV key component.
func evalLiteralToString(te TypedExpr) (string, error) {
	lit, ok := te.Expr.(*Literal)
	if !ok {
		return "", fmt.Errorf("expected literal expression, got %T", te.Expr)
	}
	switch v := lit.Value.(type) {
	case int64:
		return strconv.FormatInt(v, 10), nil
	case string:
		return v, nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case nil:
		return "null", nil
	default:
		return "", fmt.Errorf("unsupported literal type %T", lit.Value)
	}
}

// findPKValue finds the value corresponding to the primary key column among the given cols/values.
func findPKValue(table TableDef, cols []ColumnDef, values []TypedExpr) (TypedExpr, error) {
	for i, col := range cols {
		if col.Name == table.PrimaryKey {
			return values[i], nil
		}
	}
	return TypedExpr{}, fmt.Errorf("primary key column %q not found in INSERT columns", table.PrimaryKey)
}
