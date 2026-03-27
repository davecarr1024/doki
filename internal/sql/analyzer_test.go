package sql

import (
	"testing"
)

// testCatalog sets up an in-memory catalog with a "users" table for use in tests.
func testCatalog(t *testing.T) Catalog {
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
		t.Fatalf("setup: create table: %v", err)
	}
	return cat
}

func TestAnalyzeCreateTable_Happy(t *testing.T) {
	stmt, err := Parse(`CREATE TABLE products (
		id INT PRIMARY KEY NOT NULL,
		title TEXT,
		in_stock BOOL
	)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cat := NewInMemoryCatalog()
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	rc, ok := rs.(*ResolvedCreateTable)
	if !ok {
		t.Fatalf("expected *ResolvedCreateTable, got %T", rs)
	}
	if rc.Def.Name != "products" {
		t.Errorf("expected table name products, got %q", rc.Def.Name)
	}
	if rc.Def.PrimaryKey != "id" {
		t.Errorf("expected primary key id, got %q", rc.Def.PrimaryKey)
	}
}

func TestAnalyzeCreateTable_NoPrimaryKey(t *testing.T) {
	stmt, err := Parse(`CREATE TABLE nopk (id INT, name TEXT)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, NewInMemoryCatalog())
	if err == nil {
		t.Fatal("expected error for missing primary key")
	}
}

func TestAnalyzeCreateTable_DuplicateColumn(t *testing.T) {
	stmt, err := Parse(`CREATE TABLE dup (id INT PRIMARY KEY, id TEXT)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, NewInMemoryCatalog())
	if err == nil {
		t.Fatal("expected error for duplicate column name")
	}
}

func TestAnalyzeInsert_Happy(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	ri, ok := rs.(*ResolvedInsert)
	if !ok {
		t.Fatalf("expected *ResolvedInsert, got %T", rs)
	}
	if len(ri.Cols) != 3 {
		t.Errorf("expected 3 columns, got %d", len(ri.Cols))
	}
	if len(ri.Values) != 3 {
		t.Errorf("expected 3 values, got %d", len(ri.Values))
	}
	if ri.Values[0].CType != TypeInt {
		t.Errorf("expected TypeInt for id, got %v", ri.Values[0].CType)
	}
	if ri.Values[1].CType != TypeText {
		t.Errorf("expected TypeText for name, got %v", ri.Values[1].CType)
	}
	if ri.Values[2].CType != TypeBool {
		t.Errorf("expected TypeBool for active, got %v", ri.Values[2].CType)
	}
}

func TestAnalyzeInsert_NoColumnList(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`INSERT INTO users VALUES (2, 'Bob', false)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	ri, ok := rs.(*ResolvedInsert)
	if !ok {
		t.Fatalf("expected *ResolvedInsert, got %T", rs)
	}
	if len(ri.Cols) != 3 {
		t.Errorf("expected 3 columns, got %d", len(ri.Cols))
	}
}

func TestAnalyzeInsert_UnknownTable(t *testing.T) {
	stmt, err := Parse(`INSERT INTO ghost (id) VALUES (1)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, NewInMemoryCatalog())
	if err == nil {
		t.Fatal("expected error for unknown table")
	}
}

func TestAnalyzeInsert_UnknownColumn(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`INSERT INTO users (id, nonexistent) VALUES (1, 'x')`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, cat)
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestAnalyzeInsert_WrongColumnCount(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`INSERT INTO users (id, name) VALUES (1)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, cat)
	if err == nil {
		t.Fatal("expected error for wrong column count")
	}
}

func TestAnalyzeInsert_TypeMismatch(t *testing.T) {
	cat := testCatalog(t)
	// Inserting a string into an INT column
	stmt, err := Parse(`INSERT INTO users (id) VALUES ('not-an-int')`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, cat)
	if err == nil {
		t.Fatal("expected error for type mismatch")
	}
}

func TestAnalyzeSelect_Star(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`SELECT * FROM users`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	rs2, ok := rs.(*ResolvedSelect)
	if !ok {
		t.Fatalf("expected *ResolvedSelect, got %T", rs)
	}
	if rs2.Columns != nil {
		t.Errorf("expected nil columns for SELECT *, got %v", rs2.Columns)
	}
	if rs2.Where != nil {
		t.Errorf("expected nil WHERE, got %v", rs2.Where)
	}
}

func TestAnalyzeSelect_ExplicitColumns(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`SELECT id, name FROM users WHERE id = 1`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	rs2, ok := rs.(*ResolvedSelect)
	if !ok {
		t.Fatalf("expected *ResolvedSelect, got %T", rs)
	}
	if len(rs2.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(rs2.Columns))
	}
	if rs2.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if rs2.Where.PKColumn.Name != "id" {
		t.Errorf("expected pk column id, got %q", rs2.Where.PKColumn.Name)
	}
}

func TestAnalyzeSelect_UnknownTable(t *testing.T) {
	stmt, err := Parse(`SELECT * FROM ghost`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, NewInMemoryCatalog())
	if err == nil {
		t.Fatal("expected error for unknown table")
	}
}

func TestAnalyzeSelect_UnknownColumn(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`SELECT nonexistent FROM users`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, cat)
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestAnalyzeSelect_WhereNonPK(t *testing.T) {
	cat := testCatalog(t)
	// WHERE on non-PK column should be rejected in v1
	stmt, err := Parse(`SELECT * FROM users WHERE name = 'Alice'`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, cat)
	if err == nil {
		t.Fatal("expected error for WHERE on non-PK column")
	}
}

func TestAnalyzeUpdate_Happy(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`UPDATE users SET name = 'Bob' WHERE id = 1`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	ru, ok := rs.(*ResolvedUpdate)
	if !ok {
		t.Fatalf("expected *ResolvedUpdate, got %T", rs)
	}
	if len(ru.Assignments) != 1 {
		t.Errorf("expected 1 assignment, got %d", len(ru.Assignments))
	}
	if ru.Assignments[0].Column.Name != "name" {
		t.Errorf("expected column name, got %q", ru.Assignments[0].Column.Name)
	}
	if ru.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestAnalyzeUpdate_UnknownTable(t *testing.T) {
	stmt, err := Parse(`UPDATE ghost SET name = 'x' WHERE id = 1`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, NewInMemoryCatalog())
	if err == nil {
		t.Fatal("expected error for unknown table")
	}
}

func TestAnalyzeUpdate_UnknownColumn(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`UPDATE users SET nonexistent = 'x' WHERE id = 1`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, cat)
	if err == nil {
		t.Fatal("expected error for unknown column")
	}
}

func TestAnalyzeUpdate_TypeMismatch(t *testing.T) {
	cat := testCatalog(t)
	// Updating INT column with text
	stmt, err := Parse(`UPDATE users SET id = 'not-int' WHERE id = 1`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, cat)
	if err == nil {
		t.Fatal("expected error for type mismatch in assignment")
	}
}

func TestAnalyzeDelete_Happy(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`DELETE FROM users WHERE id = 42`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	rd, ok := rs.(*ResolvedDelete)
	if !ok {
		t.Fatalf("expected *ResolvedDelete, got %T", rs)
	}
	if rd.Table.Name != "users" {
		t.Errorf("expected table users, got %q", rd.Table.Name)
	}
	if rd.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if rd.Where.PKColumn.Name != "id" {
		t.Errorf("expected pk column id, got %q", rd.Where.PKColumn.Name)
	}
}

func TestAnalyzeDelete_UnknownTable(t *testing.T) {
	stmt, err := Parse(`DELETE FROM ghost WHERE id = 1`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, NewInMemoryCatalog())
	if err == nil {
		t.Fatal("expected error for unknown table")
	}
}

func TestAnalyzeDelete_WhereNonPK(t *testing.T) {
	cat := testCatalog(t)
	stmt, err := Parse(`DELETE FROM users WHERE name = 'Alice'`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	_, err = Analyze(stmt, cat)
	if err == nil {
		t.Fatal("expected error for WHERE on non-PK column")
	}
}
