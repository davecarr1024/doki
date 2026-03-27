package sql

import "fmt"

// ResolvedStatement is a Statement with catalog info looked up.
type ResolvedStatement interface{ resolvedNode() }

// ResolvedCreateTable is the resolved form of a CREATE TABLE statement.
type ResolvedCreateTable struct {
	Stmt *CreateTableStmt
	Def  TableDef
}

func (r *ResolvedCreateTable) resolvedNode() {}

// ResolvedInsert is the resolved form of an INSERT statement.
type ResolvedInsert struct {
	Table  TableDef
	Cols   []ColumnDef // ordered columns for the values
	Values []TypedExpr // same length as Cols
}

func (r *ResolvedInsert) resolvedNode() {}

// ResolvedSelect is the resolved form of a SELECT statement.
type ResolvedSelect struct {
	Table   TableDef
	Columns []ColumnDef    // columns to project; nil = all
	Where   *ResolvedWhere // nil = full scan
}

func (r *ResolvedSelect) resolvedNode() {}

// ResolvedUpdate is the resolved form of an UPDATE statement.
type ResolvedUpdate struct {
	Table       TableDef
	Assignments []ResolvedAssignment
	Where       *ResolvedWhere
}

func (r *ResolvedUpdate) resolvedNode() {}

// ResolvedDelete is the resolved form of a DELETE statement.
type ResolvedDelete struct {
	Table TableDef
	Where *ResolvedWhere
}

func (r *ResolvedDelete) resolvedNode() {}

// ResolvedWhere captures a PK equality filter.
// v1 only supports WHERE pk_col = literal.
type ResolvedWhere struct {
	PKColumn ColumnDef
	Op       string
	Value    TypedExpr
}

// ResolvedAssignment is a resolved column = value pair.
type ResolvedAssignment struct {
	Column ColumnDef
	Value  TypedExpr
}

// TypedExpr pairs an expression with its resolved type.
type TypedExpr struct {
	Expr  Expr
	CType ColumnType
}

// Analyze validates and type-checks a parsed Statement against the catalog,
// returning a ResolvedStatement or an error if the statement is semantically invalid.
func Analyze(stmt Statement, cat Catalog) (ResolvedStatement, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return analyzeCreateTable(s)
	case *InsertStmt:
		return analyzeInsert(s, cat)
	case *SelectStmt:
		return analyzeSelect(s, cat)
	case *UpdateStmt:
		return analyzeUpdate(s, cat)
	case *DeleteStmt:
		return analyzeDelete(s, cat)
	default:
		return nil, fmt.Errorf("unknown statement type %T", stmt)
	}
}

func analyzeCreateTable(stmt *CreateTableStmt) (*ResolvedCreateTable, error) {
	// Check for duplicate column names
	seen := make(map[string]bool)
	for _, col := range stmt.Columns {
		if seen[col.Name] {
			return nil, fmt.Errorf("duplicate column name %q in CREATE TABLE %q", col.Name, stmt.TableName)
		}
		seen[col.Name] = true
	}

	// Check exactly one PRIMARY KEY
	pkCount := 0
	pkName := ""
	for _, col := range stmt.Columns {
		if col.PrimaryKey {
			pkCount++
			pkName = col.Name
		}
	}
	if pkCount == 0 {
		return nil, fmt.Errorf("CREATE TABLE %q: no PRIMARY KEY defined", stmt.TableName)
	}
	if pkCount > 1 {
		return nil, fmt.Errorf("CREATE TABLE %q: multiple PRIMARY KEY columns defined", stmt.TableName)
	}

	def := TableDef{
		Name:       stmt.TableName,
		Columns:    stmt.Columns,
		PrimaryKey: pkName,
	}

	return &ResolvedCreateTable{Stmt: stmt, Def: def}, nil
}

func analyzeInsert(stmt *InsertStmt, cat Catalog) (*ResolvedInsert, error) {
	table, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("INSERT: %w", err)
	}

	// Determine target columns
	var cols []ColumnDef
	if len(stmt.Columns) == 0 {
		// No column list: use all columns in order
		cols = table.Columns
	} else {
		for _, name := range stmt.Columns {
			col, ok := findColumn(table, name)
			if !ok {
				return nil, fmt.Errorf("INSERT: column %q not found in table %q", name, stmt.TableName)
			}
			cols = append(cols, col)
		}
	}

	// Check column count matches value count
	if len(stmt.Values) != len(cols) {
		return nil, fmt.Errorf("INSERT: %d values provided but table has %d columns", len(stmt.Values), len(cols))
	}

	// Type-check each value
	typedVals := make([]TypedExpr, len(cols))
	for i, val := range stmt.Values {
		tv, err := resolveExpr(val, table)
		if err != nil {
			return nil, fmt.Errorf("INSERT: value %d: %w", i, err)
		}
		if err := checkTypeCompat(tv.CType, cols[i].Type, cols[i].Name); err != nil {
			return nil, fmt.Errorf("INSERT: %w", err)
		}
		typedVals[i] = tv
	}

	return &ResolvedInsert{
		Table:  table,
		Cols:   cols,
		Values: typedVals,
	}, nil
}

func analyzeSelect(stmt *SelectStmt, cat Catalog) (*ResolvedSelect, error) {
	table, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("SELECT: %w", err)
	}

	// Resolve column list
	var columns []ColumnDef
	if len(stmt.Columns) == 0 {
		// nil columns: select all
		columns = nil
	} else if len(stmt.Columns) == 1 {
		if _, ok := stmt.Columns[0].(*StarExpr); ok {
			// SELECT * -> nil (all columns)
			columns = nil
		} else {
			col, err := resolveColumnExpr(stmt.Columns[0], table)
			if err != nil {
				return nil, fmt.Errorf("SELECT: %w", err)
			}
			columns = []ColumnDef{col}
		}
	} else {
		for _, expr := range stmt.Columns {
			if _, ok := expr.(*StarExpr); ok {
				return nil, fmt.Errorf("SELECT: * cannot be mixed with other columns")
			}
			col, err := resolveColumnExpr(expr, table)
			if err != nil {
				return nil, fmt.Errorf("SELECT: %w", err)
			}
			columns = append(columns, col)
		}
	}

	// Resolve WHERE
	var where *ResolvedWhere
	if stmt.Where != nil {
		where, err = resolveWhere(stmt.Where, table)
		if err != nil {
			return nil, fmt.Errorf("SELECT: %w", err)
		}
	}

	return &ResolvedSelect{
		Table:   table,
		Columns: columns,
		Where:   where,
	}, nil
}

func analyzeUpdate(stmt *UpdateStmt, cat Catalog) (*ResolvedUpdate, error) {
	table, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("UPDATE: %w", err)
	}

	// Resolve assignments
	assignments := make([]ResolvedAssignment, len(stmt.Assignments))
	for i, a := range stmt.Assignments {
		col, ok := findColumn(table, a.Column)
		if !ok {
			return nil, fmt.Errorf("UPDATE: column %q not found in table %q", a.Column, stmt.TableName)
		}
		tv, err := resolveExpr(a.Value, table)
		if err != nil {
			return nil, fmt.Errorf("UPDATE: assignment to %q: %w", a.Column, err)
		}
		if err := checkTypeCompat(tv.CType, col.Type, col.Name); err != nil {
			return nil, fmt.Errorf("UPDATE: %w", err)
		}
		assignments[i] = ResolvedAssignment{Column: col, Value: tv}
	}

	// Resolve WHERE (required for UPDATE in v1)
	if stmt.Where == nil {
		return nil, fmt.Errorf("UPDATE: WHERE clause required in v1")
	}
	where, err := resolveWhere(stmt.Where, table)
	if err != nil {
		return nil, fmt.Errorf("UPDATE: %w", err)
	}

	return &ResolvedUpdate{
		Table:       table,
		Assignments: assignments,
		Where:       where,
	}, nil
}

func analyzeDelete(stmt *DeleteStmt, cat Catalog) (*ResolvedDelete, error) {
	table, err := cat.GetTable(stmt.TableName)
	if err != nil {
		return nil, fmt.Errorf("DELETE: %w", err)
	}

	// Resolve WHERE (required for DELETE in v1)
	if stmt.Where == nil {
		return nil, fmt.Errorf("DELETE: WHERE clause required in v1")
	}
	where, err := resolveWhere(stmt.Where, table)
	if err != nil {
		return nil, fmt.Errorf("DELETE: %w", err)
	}

	return &ResolvedDelete{
		Table: table,
		Where: where,
	}, nil
}

// resolveWhere resolves a WHERE expression. In v1, only pk_col = literal is supported.
func resolveWhere(expr Expr, table TableDef) (*ResolvedWhere, error) {
	be, ok := expr.(*BinaryExpr)
	if !ok {
		return nil, fmt.Errorf("v1: WHERE must filter on primary key column with =")
	}

	colRef, ok := be.Left.(*ColumnRef)
	if !ok {
		return nil, fmt.Errorf("v1: WHERE must filter on primary key column with =")
	}

	if colRef.Name != table.PrimaryKey {
		return nil, fmt.Errorf("v1: WHERE must filter on primary key column with =")
	}

	if be.Op != "=" {
		return nil, fmt.Errorf("v1: WHERE must filter on primary key column with =")
	}

	pkCol, ok := findColumn(table, table.PrimaryKey)
	if !ok {
		return nil, fmt.Errorf("internal: primary key column %q not found in table %q", table.PrimaryKey, table.Name)
	}

	tv, err := resolveExpr(be.Right, table)
	if err != nil {
		return nil, fmt.Errorf("WHERE value: %w", err)
	}
	if err := checkTypeCompat(tv.CType, pkCol.Type, pkCol.Name); err != nil {
		return nil, fmt.Errorf("WHERE: %w", err)
	}

	return &ResolvedWhere{
		PKColumn: pkCol,
		Op:       be.Op,
		Value:    tv,
	}, nil
}

// resolveColumnExpr resolves an Expr that should be a ColumnRef to a ColumnDef.
func resolveColumnExpr(expr Expr, table TableDef) (ColumnDef, error) {
	ref, ok := expr.(*ColumnRef)
	if !ok {
		return ColumnDef{}, fmt.Errorf("expected column name, got %T", expr)
	}
	col, found := findColumn(table, ref.Name)
	if !found {
		return ColumnDef{}, fmt.Errorf("column %q not found in table %q", ref.Name, table.Name)
	}
	return col, nil
}

// resolveExpr resolves an expression to a TypedExpr.
func resolveExpr(expr Expr, table TableDef) (TypedExpr, error) {
	switch e := expr.(type) {
	case *Literal:
		return resolveLiteral(e)
	case *ColumnRef:
		col, ok := findColumn(table, e.Name)
		if !ok {
			return TypedExpr{}, fmt.Errorf("column %q not found in table %q", e.Name, table.Name)
		}
		return TypedExpr{Expr: e, CType: col.Type}, nil
	default:
		return TypedExpr{}, fmt.Errorf("unsupported expression type %T", expr)
	}
}

// resolveLiteral infers the ColumnType of a literal value.
func resolveLiteral(lit *Literal) (TypedExpr, error) {
	switch lit.Value.(type) {
	case int64:
		return TypedExpr{Expr: lit, CType: TypeInt}, nil
	case string:
		return TypedExpr{Expr: lit, CType: TypeText}, nil
	case bool:
		return TypedExpr{Expr: lit, CType: TypeBool}, nil
	case nil:
		// NULL is compatible with any type; use TypeInt as placeholder
		return TypedExpr{Expr: lit, CType: TypeInt}, nil
	default:
		return TypedExpr{}, fmt.Errorf("unsupported literal type %T", lit.Value)
	}
}

// checkTypeCompat checks that a given type is compatible with an expected column type.
func checkTypeCompat(actual, expected ColumnType, colName string) error {
	if actual == expected {
		return nil
	}
	// NULL literal is compatible with any type
	return fmt.Errorf("type mismatch for column %q: expected %s, got %s", colName, columnTypeName(expected), columnTypeName(actual))
}

// columnTypeName returns a human-readable name for a ColumnType.
func columnTypeName(ct ColumnType) string {
	switch ct {
	case TypeInt:
		return "INT"
	case TypeText:
		return "TEXT"
	case TypeBool:
		return "BOOL"
	default:
		return "UNKNOWN"
	}
}

// findColumn looks up a column by name in a TableDef.
func findColumn(table TableDef, name string) (ColumnDef, bool) {
	for _, col := range table.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return ColumnDef{}, false
}
