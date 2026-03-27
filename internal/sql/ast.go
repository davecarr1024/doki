package sql

// Statement is the interface implemented by all SQL statement AST nodes.
type Statement interface {
	statementNode()
}

// Expr is the interface implemented by all SQL expression AST nodes.
type Expr interface {
	exprNode()
}

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

func (s *CreateTableStmt) statementNode() {}

// InsertStmt represents an INSERT INTO statement.
type InsertStmt struct {
	TableName string
	Columns   []string // nil means all columns in order
	Values    []Expr
}

func (s *InsertStmt) statementNode() {}

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Columns   []Expr // nil or [StarExpr{}] means SELECT *
	TableName string
	Where     Expr // nil means no WHERE clause
}

func (s *SelectStmt) statementNode() {}

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	TableName   string
	Assignments []Assignment
	Where       Expr
}

func (s *UpdateStmt) statementNode() {}

// DeleteStmt represents a DELETE FROM statement.
type DeleteStmt struct {
	TableName string
	Where     Expr
}

func (s *DeleteStmt) statementNode() {}

// Assignment represents a column = expr pair in an UPDATE SET clause.
type Assignment struct {
	Column string
	Value  Expr
}

// BinaryExpr represents a binary expression such as a = b or x AND y.
type BinaryExpr struct {
	Left  Expr
	Op    string // "=", "!=", "<", ">", "<=", ">=", "AND", "OR"
	Right Expr
}

func (e *BinaryExpr) exprNode() {}

// ColumnRef represents a reference to a column by name.
type ColumnRef struct {
	Name string
}

func (e *ColumnRef) exprNode() {}

// Literal represents a constant value: int64, string, bool, or nil.
type Literal struct {
	Value any // int64, string, bool, or nil
}

func (e *Literal) exprNode() {}

// StarExpr represents the * wildcard in SELECT *.
type StarExpr struct{}

func (e *StarExpr) exprNode() {}
