package sql

import (
	"strings"
	"sync"
	"testing"
)

// inMemoryKV is a simple thread-safe in-memory KV store for testing.
type inMemoryKV struct {
	mu   sync.RWMutex
	data map[string]string
}

func newInMemoryKV() *inMemoryKV {
	return &inMemoryKV{data: make(map[string]string)}
}

func (k *inMemoryKV) Get(key string) (string, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.data[key]
	if !ok {
		return "", ErrNotFound
	}
	return v, nil
}

func (k *inMemoryKV) Put(key, value string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.data[key] = value
	return nil
}

func (k *inMemoryKV) Delete(key string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.data, key)
	return nil
}

func (k *inMemoryKV) Scan(prefix string) ([]KVEntry, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	var entries []KVEntry
	for key, val := range k.data {
		if strings.HasPrefix(key, prefix) {
			entries = append(entries, KVEntry{Key: key, Value: val})
		}
	}
	return entries, nil
}

// runSQL is a helper that runs the full Parse → Analyze → Plan → Execute pipeline.
func runSQL(t *testing.T, query string, kv KV, cat Catalog) *ResultSet {
	t.Helper()
	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("parse %q: %v", query, err)
	}
	rs, err := Analyze(stmt, cat)
	if err != nil {
		t.Fatalf("analyze %q: %v", query, err)
	}
	plan, err := Plan(rs)
	if err != nil {
		t.Fatalf("plan %q: %v", query, err)
	}
	result, err := Execute(plan, kv, cat)
	if err != nil {
		t.Fatalf("execute %q: %v", query, err)
	}
	return result
}

// executorTestSetup creates a catalog with a "users" table and an in-memory KV.
func executorTestSetup(t *testing.T) (Catalog, *inMemoryKV) {
	t.Helper()
	cat := NewInMemoryCatalog()
	kv := newInMemoryKV()
	runSQL(t, `CREATE TABLE users (id INT PRIMARY KEY NOT NULL, name TEXT, active BOOL)`, kv, cat)
	return cat, kv
}

func TestExecute_CreateTable(t *testing.T) {
	cat := NewInMemoryCatalog()
	kv := newInMemoryKV()
	result := runSQL(t, `CREATE TABLE widgets (id INT PRIMARY KEY NOT NULL, label TEXT)`, kv, cat)
	if result.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected, got %d", result.RowsAffected)
	}
	_, err := cat.GetTable("widgets")
	if err != nil {
		t.Errorf("expected table widgets to exist in catalog: %v", err)
	}
}

func TestExecute_InsertThenSelectStar(t *testing.T) {
	cat, kv := executorTestSetup(t)

	runSQL(t, `INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)`, kv, cat)
	result := runSQL(t, `SELECT * FROM users`, kv, cat)

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	row := result.Rows[0]
	// JSON decoding returns numbers as float64; compare accordingly
	idVal, ok := row["id"]
	if !ok {
		t.Error("expected id in result")
	}
	// JSON decode turns int64 -> float64
	if idVal != float64(1) {
		t.Errorf("expected id=1, got %v (%T)", idVal, idVal)
	}
	if row["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", row["name"])
	}
	if row["active"] != true {
		t.Errorf("expected active=true, got %v", row["active"])
	}
}

func TestExecute_InsertThenSelectWherePK(t *testing.T) {
	cat, kv := executorTestSetup(t)

	runSQL(t, `INSERT INTO users (id, name, active) VALUES (42, 'Bob', false)`, kv, cat)
	runSQL(t, `INSERT INTO users (id, name, active) VALUES (99, 'Charlie', true)`, kv, cat)

	result := runSQL(t, `SELECT * FROM users WHERE id = 42`, kv, cat)
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0]["name"] != "Bob" {
		t.Errorf("expected name=Bob, got %v", result.Rows[0]["name"])
	}
}

func TestExecute_InsertUpdateSelect(t *testing.T) {
	cat, kv := executorTestSetup(t)

	runSQL(t, `INSERT INTO users (id, name, active) VALUES (10, 'Dave', false)`, kv, cat)
	updateResult := runSQL(t, `UPDATE users SET name = 'David' WHERE id = 10`, kv, cat)
	if updateResult.RowsAffected != 1 {
		t.Errorf("expected 1 row affected by UPDATE, got %d", updateResult.RowsAffected)
	}

	result := runSQL(t, `SELECT * FROM users WHERE id = 10`, kv, cat)
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0]["name"] != "David" {
		t.Errorf("expected name=David after update, got %v", result.Rows[0]["name"])
	}
}

func TestExecute_InsertDeleteSelect(t *testing.T) {
	cat, kv := executorTestSetup(t)

	runSQL(t, `INSERT INTO users (id, name, active) VALUES (7, 'Eve', true)`, kv, cat)
	deleteResult := runSQL(t, `DELETE FROM users WHERE id = 7`, kv, cat)
	if deleteResult.RowsAffected != 1 {
		t.Errorf("expected 1 row affected by DELETE, got %d", deleteResult.RowsAffected)
	}

	result := runSQL(t, `SELECT * FROM users WHERE id = 7`, kv, cat)
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows after delete, got %d", len(result.Rows))
	}
}

func TestExecute_SelectEmptyTable(t *testing.T) {
	cat, kv := executorTestSetup(t)

	result := runSQL(t, `SELECT * FROM users`, kv, cat)
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows on empty table, got %d", len(result.Rows))
	}
}

func TestExecute_SelectExplicitColumns(t *testing.T) {
	cat, kv := executorTestSetup(t)

	runSQL(t, `INSERT INTO users (id, name, active) VALUES (3, 'Frank', true)`, kv, cat)
	result := runSQL(t, `SELECT id, name FROM users WHERE id = 3`, kv, cat)

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	row := result.Rows[0]
	if _, hasActive := row["active"]; hasActive {
		t.Error("expected active column to be excluded from projection")
	}
	if row["name"] != "Frank" {
		t.Errorf("expected name=Frank, got %v", row["name"])
	}
	// Verify column list
	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "id" || result.Columns[1] != "name" {
		t.Errorf("expected columns [id name], got %v", result.Columns)
	}
}

func TestExecute_UpdateNonexistentRow(t *testing.T) {
	cat, kv := executorTestSetup(t)

	result := runSQL(t, `UPDATE users SET name = 'Ghost' WHERE id = 999`, kv, cat)
	if result.RowsAffected != 0 {
		t.Errorf("expected 0 rows affected for UPDATE of nonexistent row, got %d", result.RowsAffected)
	}
}

func TestExecute_SelectMultipleRows(t *testing.T) {
	cat, kv := executorTestSetup(t)

	runSQL(t, `INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)`, kv, cat)
	runSQL(t, `INSERT INTO users (id, name, active) VALUES (2, 'Bob', false)`, kv, cat)
	runSQL(t, `INSERT INTO users (id, name, active) VALUES (3, 'Carol', true)`, kv, cat)

	result := runSQL(t, `SELECT * FROM users`, kv, cat)
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestExecute_TypePreservation(t *testing.T) {
	cat := NewInMemoryCatalog()
	kv := newInMemoryKV()

	// Create a table with int, text, and bool columns
	runSQL(t, `CREATE TABLE things (id INT PRIMARY KEY NOT NULL, label TEXT, flag BOOL)`, kv, cat)
	runSQL(t, `INSERT INTO things (id, label, flag) VALUES (100, 'hello', true)`, kv, cat)

	result := runSQL(t, `SELECT * FROM things WHERE id = 100`, kv, cat)
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	row := result.Rows[0]

	// JSON round-trip converts int64 -> float64 and bool stays bool, string stays string
	if row["id"] != float64(100) {
		t.Errorf("expected id=float64(100), got %v (%T)", row["id"], row["id"])
	}
	if row["label"] != "hello" {
		t.Errorf("expected label=hello, got %v (%T)", row["label"], row["label"])
	}
	if row["flag"] != true {
		t.Errorf("expected flag=true, got %v (%T)", row["flag"], row["flag"])
	}
}
