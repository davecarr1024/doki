package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse_CreateTable(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, active BOOL)")
	require.NoError(t, err)

	ct, ok := stmt.(*CreateTableStmt)
	require.True(t, ok, "expected *CreateTableStmt")
	assert.Equal(t, "users", ct.TableName)
	require.Len(t, ct.Columns, 3)

	assert.Equal(t, "id", ct.Columns[0].Name)
	assert.Equal(t, TypeInt, ct.Columns[0].Type)
	assert.True(t, ct.Columns[0].PrimaryKey)
	assert.False(t, ct.Columns[0].NotNull)

	assert.Equal(t, "name", ct.Columns[1].Name)
	assert.Equal(t, TypeText, ct.Columns[1].Type)
	assert.False(t, ct.Columns[1].PrimaryKey)
	assert.True(t, ct.Columns[1].NotNull)

	assert.Equal(t, "active", ct.Columns[2].Name)
	assert.Equal(t, TypeBool, ct.Columns[2].Type)
	assert.False(t, ct.Columns[2].PrimaryKey)
	assert.False(t, ct.Columns[2].NotNull)
}

func TestParse_CreateTable_WithSemicolon(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (id INT PRIMARY KEY);")
	require.NoError(t, err)
	ct, ok := stmt.(*CreateTableStmt)
	require.True(t, ok)
	assert.Equal(t, "t", ct.TableName)
}

func TestParse_InsertAllColumns(t *testing.T) {
	stmt, err := Parse("INSERT INTO users VALUES (1, 'alice', true)")
	require.NoError(t, err)

	ins, ok := stmt.(*InsertStmt)
	require.True(t, ok)
	assert.Equal(t, "users", ins.TableName)
	assert.Nil(t, ins.Columns)
	require.Len(t, ins.Values, 3)

	lit0, ok := ins.Values[0].(*Literal)
	require.True(t, ok)
	assert.Equal(t, int64(1), lit0.Value)

	lit1, ok := ins.Values[1].(*Literal)
	require.True(t, ok)
	assert.Equal(t, "alice", lit1.Value)

	lit2, ok := ins.Values[2].(*Literal)
	require.True(t, ok)
	assert.Equal(t, true, lit2.Value)
}

func TestParse_InsertNamedColumns(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (id, name) VALUES (1, 'bob')")
	require.NoError(t, err)

	ins, ok := stmt.(*InsertStmt)
	require.True(t, ok)
	assert.Equal(t, "users", ins.TableName)
	assert.Equal(t, []string{"id", "name"}, ins.Columns)
	require.Len(t, ins.Values, 2)
}

func TestParse_SelectStar(t *testing.T) {
	stmt, err := Parse("SELECT * FROM users")
	require.NoError(t, err)

	sel, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	assert.Equal(t, "users", sel.TableName)
	require.Len(t, sel.Columns, 1)
	_, ok = sel.Columns[0].(*StarExpr)
	assert.True(t, ok)
	assert.Nil(t, sel.Where)
}

func TestParse_SelectColumnsWithWhere(t *testing.T) {
	stmt, err := Parse("SELECT id, name FROM users WHERE id = 42")
	require.NoError(t, err)

	sel, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	assert.Equal(t, "users", sel.TableName)
	require.Len(t, sel.Columns, 2)

	col0, ok := sel.Columns[0].(*ColumnRef)
	require.True(t, ok)
	assert.Equal(t, "id", col0.Name)

	col1, ok := sel.Columns[1].(*ColumnRef)
	require.True(t, ok)
	assert.Equal(t, "name", col1.Name)

	require.NotNil(t, sel.Where)
	binExpr, ok := sel.Where.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, "=", binExpr.Op)

	leftRef, ok := binExpr.Left.(*ColumnRef)
	require.True(t, ok)
	assert.Equal(t, "id", leftRef.Name)

	rightLit, ok := binExpr.Right.(*Literal)
	require.True(t, ok)
	assert.Equal(t, int64(42), rightLit.Value)
}

func TestParse_Update(t *testing.T) {
	stmt, err := Parse("UPDATE users SET name = 'bob', active = false WHERE id = 1")
	require.NoError(t, err)

	upd, ok := stmt.(*UpdateStmt)
	require.True(t, ok)
	assert.Equal(t, "users", upd.TableName)
	require.Len(t, upd.Assignments, 2)

	assert.Equal(t, "name", upd.Assignments[0].Column)
	nameLit, ok := upd.Assignments[0].Value.(*Literal)
	require.True(t, ok)
	assert.Equal(t, "bob", nameLit.Value)

	assert.Equal(t, "active", upd.Assignments[1].Column)
	activeLit, ok := upd.Assignments[1].Value.(*Literal)
	require.True(t, ok)
	assert.Equal(t, false, activeLit.Value)

	require.NotNil(t, upd.Where)
	whereExpr, ok := upd.Where.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, "=", whereExpr.Op)
}

func TestParse_Delete(t *testing.T) {
	stmt, err := Parse("DELETE FROM users WHERE id = 99")
	require.NoError(t, err)

	del, ok := stmt.(*DeleteStmt)
	require.True(t, ok)
	assert.Equal(t, "users", del.TableName)

	require.NotNil(t, del.Where)
	whereExpr, ok := del.Where.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, "=", whereExpr.Op)

	leftRef, ok := whereExpr.Left.(*ColumnRef)
	require.True(t, ok)
	assert.Equal(t, "id", leftRef.Name)

	rightLit, ok := whereExpr.Right.(*Literal)
	require.True(t, ok)
	assert.Equal(t, int64(99), rightLit.Value)
}

func TestParse_NullLiteral(t *testing.T) {
	stmt, err := Parse("INSERT INTO t VALUES (NULL)")
	require.NoError(t, err)
	ins, ok := stmt.(*InsertStmt)
	require.True(t, ok)
	require.Len(t, ins.Values, 1)
	lit, ok := ins.Values[0].(*Literal)
	require.True(t, ok)
	assert.Nil(t, lit.Value)
}

func TestParse_BinaryAndExpression(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE a = 1 AND b = 2")
	require.NoError(t, err)
	sel, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	require.NotNil(t, sel.Where)
	binExpr, ok := sel.Where.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, "AND", binExpr.Op)
}

func TestParse_BinaryOrExpression(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE a = 1 OR b = 2")
	require.NoError(t, err)
	sel, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	require.NotNil(t, sel.Where)
	binExpr, ok := sel.Where.(*BinaryExpr)
	require.True(t, ok)
	assert.Equal(t, "OR", binExpr.Op)
}

func TestParse_ComparisonOperators(t *testing.T) {
	ops := []string{"!=", "<", ">", "<=", ">="}
	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			stmt, err := Parse("SELECT * FROM t WHERE id " + op + " 5")
			require.NoError(t, err)
			sel, ok := stmt.(*SelectStmt)
			require.True(t, ok)
			binExpr, ok := sel.Where.(*BinaryExpr)
			require.True(t, ok)
			assert.Equal(t, op, binExpr.Op)
		})
	}
}

func TestParse_ParenthesizedExpr(t *testing.T) {
	stmt, err := Parse("SELECT * FROM t WHERE (id = 1)")
	require.NoError(t, err)
	sel, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	require.NotNil(t, sel.Where)
	_, ok = sel.Where.(*BinaryExpr)
	assert.True(t, ok)
}

func TestParse_Error_UnexpectedToken(t *testing.T) {
	_, err := Parse("INVALID something")
	assert.Error(t, err)
}

func TestParse_Error_MissingTableName(t *testing.T) {
	_, err := Parse("SELECT * FROM")
	assert.Error(t, err)
}

func TestParse_Error_MissingCloseParen(t *testing.T) {
	_, err := Parse("CREATE TABLE t (id INT PRIMARY KEY")
	assert.Error(t, err)
}

func TestParse_Error_MissingValues(t *testing.T) {
	_, err := Parse("INSERT INTO t")
	assert.Error(t, err)
}

func TestParse_Error_SyntaxInExpr(t *testing.T) {
	_, err := Parse("SELECT * FROM t WHERE =")
	assert.Error(t, err)
}

func TestParse_CaseInsensitiveKeywords(t *testing.T) {
	stmt, err := Parse("select * from users")
	require.NoError(t, err)
	sel, ok := stmt.(*SelectStmt)
	require.True(t, ok)
	assert.Equal(t, "users", sel.TableName)
}
