package sql

import (
	"testing"
)

// plannerTestCatalog creates a catalog for planner tests with a "users" table.
func plannerTestCatalog(t *testing.T) Catalog {
	t.Helper()
	cat := NewInMemoryCatalog()
	err := cat.CreateTable(TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: TypeInt, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: TypeText},
			{Name: "active", Type: TypeBool},
		},
		PrimaryKey: "id",
	})
	if err != nil {
		t.Fatalf("setup catalog: %v", err)
	}
	return cat
}

// parseAnalyze is a helper that runs Parse then Analyze.
func parseAnalyze(t *testing.T, query string, cat Catalog) ResolvedStatement {
	t.Helper()
	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze %q: %v", query, err)
	}
	return rs
}

func TestPlanCreateTable(t *testing.T) {
	cat := NewInMemoryCatalog()
	rs := parseAnalyze(t, `CREATE TABLE products (id INT PRIMARY KEY NOT NULL, title TEXT)`, cat)
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cp, ok := plan.(*CreateTablePlan)
	if !ok {
		t.Fatalf("expected *CreateTablePlan, got %T", plan)
	}
	if cp.Def.Name != "products" {
		t.Errorf("expected table products, got %q", cp.Def.Name)
	}
}

func TestPlanInsert(t *testing.T) {
	cat := plannerTestCatalog(t)
	rs := parseAnalyze(t, `INSERT INTO users (id, name, active) VALUES (42, 'Alice', true)`, cat)
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	ip, ok := plan.(*InsertPlan)
	if !ok {
		t.Fatalf("expected *InsertPlan, got %T", plan)
	}
	if ip.Key != "users/42" {
		t.Errorf("expected key users/42, got %q", ip.Key)
	}
	if ip.Row["id"] != int64(42) {
		t.Errorf("expected id=42, got %v", ip.Row["id"])
	}
	if ip.Row["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", ip.Row["name"])
	}
	if ip.Row["active"] != true {
		t.Errorf("expected active=true, got %v", ip.Row["active"])
	}
}

func TestPlanSelectStar(t *testing.T) {
	cat := plannerTestCatalog(t)
	rs := parseAnalyze(t, `SELECT * FROM users`, cat)
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	ts, ok := plan.(*TableScan)
	if !ok {
		t.Fatalf("expected *TableScan, got %T", plan)
	}
	if ts.Prefix != "users/" {
		t.Errorf("expected prefix users/, got %q", ts.Prefix)
	}
	if ts.Columns != nil {
		t.Errorf("expected nil columns for SELECT *, got %v", ts.Columns)
	}
}

func TestPlanSelectWhereEq(t *testing.T) {
	cat := plannerTestCatalog(t)
	rs := parseAnalyze(t, `SELECT * FROM users WHERE id = 42`, cat)
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	pg, ok := plan.(*PointGet)
	if !ok {
		t.Fatalf("expected *PointGet, got %T", plan)
	}
	if pg.Key != "users/42" {
		t.Errorf("expected key users/42, got %q", pg.Key)
	}
}

func TestPlanSelectExplicitColumns(t *testing.T) {
	cat := plannerTestCatalog(t)
	rs := parseAnalyze(t, `SELECT id, name FROM users WHERE id = 1`, cat)
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	pg, ok := plan.(*PointGet)
	if !ok {
		t.Fatalf("expected *PointGet, got %T", plan)
	}
	if pg.Key != "users/1" {
		t.Errorf("expected key users/1, got %q", pg.Key)
	}
	if len(pg.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(pg.Columns))
	}
}

func TestPlanUpdate(t *testing.T) {
	cat := plannerTestCatalog(t)
	rs := parseAnalyze(t, `UPDATE users SET name = 'Bob' WHERE id = 10`, cat)
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	pu, ok := plan.(*PointUpdate)
	if !ok {
		t.Fatalf("expected *PointUpdate, got %T", plan)
	}
	if pu.Key != "users/10" {
		t.Errorf("expected key users/10, got %q", pu.Key)
	}
	if len(pu.Assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(pu.Assignments))
	}
	if pu.Assignments[0].Column.Name != "name" {
		t.Errorf("expected assignment to name, got %q", pu.Assignments[0].Column.Name)
	}
}

func TestPlanDelete(t *testing.T) {
	cat := plannerTestCatalog(t)
	rs := parseAnalyze(t, `DELETE FROM users WHERE id = 5`, cat)
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	pd, ok := plan.(*PointDelete)
	if !ok {
		t.Fatalf("expected *PointDelete, got %T", plan)
	}
	if pd.Key != "users/5" {
		t.Errorf("expected key users/5, got %q", pd.Key)
	}
}

func TestPlanInsert_StringPK(t *testing.T) {
	cat := NewInMemoryCatalog()
	err := cat.CreateTable(TableDef{
		Name: "items",
		Columns: []ColumnDef{
			{Name: "slug", Type: TypeText, PrimaryKey: true},
			{Name: "value", Type: TypeInt},
		},
		PrimaryKey: "slug",
	})
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	rs := parseAnalyze(t, `INSERT INTO items (slug, value) VALUES ('hello', 99)`, cat)
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	ip, ok := plan.(*InsertPlan)
	if !ok {
		t.Fatalf("expected *InsertPlan, got %T", plan)
	}
	if ip.Key != "items/hello" {
		t.Errorf("expected key items/hello, got %q", ip.Key)
	}
}
