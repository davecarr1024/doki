package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse tokenizes and parses the input SQL string, returning a Statement AST node.
func Parse(input string) (Statement, error) {
	tokens, err := Lex(input)
	if err != nil {
		return nil, fmt.Errorf("lex: %w", err)
	}
	p := &parser{tokens: tokens, pos: 0}
	return p.parseStatement()
}

type parser struct {
	tokens []Token
	pos    int
}

// peek returns the current token without advancing.
func (p *parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TOKEN_EOF}
	}
	return p.tokens[p.pos]
}

// advance returns the current token and moves forward.
func (p *parser) advance() Token {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

// expect consumes a token of the given type or returns an error.
func (p *parser) expect(tt TokenType) (Token, error) {
	tok := p.advance()
	if tok.Type != tt {
		return Token{}, fmt.Errorf("expected %s but got %s (%q) at position %d", tt, tok.Type, tok.Literal, tok.Pos)
	}
	return tok, nil
}

// check returns true if the current token has the given type without consuming it.
func (p *parser) check(tt TokenType) bool {
	return p.peek().Type == tt
}

// match consumes the current token if it matches one of the given types.
func (p *parser) match(types ...TokenType) bool {
	for _, tt := range types {
		if p.check(tt) {
			p.advance()
			return true
		}
	}
	return false
}

// parseStatement dispatches to the appropriate statement parser.
func (p *parser) parseStatement() (Statement, error) {
	tok := p.peek()
	switch tok.Type {
	case TOKEN_CREATE:
		return p.parseCreateTable()
	case TOKEN_INSERT:
		return p.parseInsert()
	case TOKEN_SELECT:
		return p.parseSelect()
	case TOKEN_UPDATE:
		return p.parseUpdate()
	case TOKEN_DELETE:
		return p.parseDelete()
	default:
		return nil, fmt.Errorf("unexpected token %s (%q) at position %d; expected SELECT, INSERT, UPDATE, DELETE, or CREATE", tok.Type, tok.Literal, tok.Pos)
	}
}

// parseCreateTable parses: CREATE TABLE ident ( col_def (, col_def)* ) ;?
func (p *parser) parseCreateTable() (Statement, error) {
	if _, err := p.expect(TOKEN_CREATE); err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_TABLE); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(TOKEN_IDENT)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}

	var cols []ColumnDef
	for {
		col, err := p.parseColumnDef()
		if err != nil {
			return nil, err
		}
		cols = append(cols, col)
		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	if _, err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	// optional semicolon
	p.match(TOKEN_SEMICOLON)

	return &CreateTableStmt{
		TableName: nameTok.Literal,
		Columns:   cols,
	}, nil
}

// parseColumnDef parses: ident type (PRIMARY KEY)? (NOT NULL)?
func (p *parser) parseColumnDef() (ColumnDef, error) {
	nameTok, err := p.expect(TOKEN_IDENT)
	if err != nil {
		return ColumnDef{}, err
	}

	colType, err := p.parseColumnType()
	if err != nil {
		return ColumnDef{}, err
	}

	var primaryKey bool
	var notNull bool

	// parse optional column constraints in any order
	for {
		if p.check(TOKEN_PRIMARY) {
			p.advance()
			if _, err := p.expect(TOKEN_KEY); err != nil {
				return ColumnDef{}, err
			}
			primaryKey = true
		} else if p.check(TOKEN_NOT) {
			p.advance()
			if _, err := p.expect(TOKEN_NULL); err != nil {
				return ColumnDef{}, err
			}
			notNull = true
		} else {
			break
		}
	}

	return ColumnDef{
		Name:       nameTok.Literal,
		Type:       colType,
		PrimaryKey: primaryKey,
		NotNull:    notNull,
	}, nil
}

// parseColumnType parses INT | TEXT | BOOL.
func (p *parser) parseColumnType() (ColumnType, error) {
	tok := p.advance()
	switch tok.Type {
	case TOKEN_INT:
		return TypeInt, nil
	case TOKEN_TEXT:
		return TypeText, nil
	case TOKEN_BOOL:
		return TypeBool, nil
	default:
		return 0, fmt.Errorf("expected column type (INT, TEXT, BOOL) but got %s (%q) at position %d", tok.Type, tok.Literal, tok.Pos)
	}
}

// parseInsert parses: INSERT INTO ident [(ident,...)] VALUES (expr,...) ;?
func (p *parser) parseInsert() (Statement, error) {
	if _, err := p.expect(TOKEN_INSERT); err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_INTO); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(TOKEN_IDENT)
	if err != nil {
		return nil, err
	}

	// optional column list
	var cols []string
	if p.check(TOKEN_LPAREN) {
		p.advance()
		for {
			colTok, err := p.expect(TOKEN_IDENT)
			if err != nil {
				return nil, err
			}
			cols = append(cols, colTok.Literal)
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
		if _, err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(TOKEN_VALUES); err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_LPAREN); err != nil {
		return nil, err
	}

	var values []Expr
	for {
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		values = append(values, val)
		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	if _, err := p.expect(TOKEN_RPAREN); err != nil {
		return nil, err
	}
	p.match(TOKEN_SEMICOLON)

	return &InsertStmt{
		TableName: nameTok.Literal,
		Columns:   cols,
		Values:    values,
	}, nil
}

// parseSelect parses: SELECT (star | ident,...) FROM ident [WHERE expr] ;?
func (p *parser) parseSelect() (Statement, error) {
	if _, err := p.expect(TOKEN_SELECT); err != nil {
		return nil, err
	}

	var columns []Expr
	if p.check(TOKEN_STAR) {
		p.advance()
		columns = []Expr{&StarExpr{}}
	} else {
		// parse comma-separated column list
		for {
			col, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			columns = append(columns, col)
			if !p.match(TOKEN_COMMA) {
				break
			}
		}
	}

	if _, err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(TOKEN_IDENT)
	if err != nil {
		return nil, err
	}

	var where Expr
	if p.match(TOKEN_WHERE) {
		where, err = p.parseExpr()
		if err != nil {
			return nil, err
		}
	}

	p.match(TOKEN_SEMICOLON)

	return &SelectStmt{
		Columns:   columns,
		TableName: nameTok.Literal,
		Where:     where,
	}, nil
}

// parseUpdate parses: UPDATE ident SET assignment (,assignment)* WHERE expr ;?
func (p *parser) parseUpdate() (Statement, error) {
	if _, err := p.expect(TOKEN_UPDATE); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(TOKEN_IDENT)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_SET); err != nil {
		return nil, err
	}

	var assignments []Assignment
	for {
		colTok, err := p.expect(TOKEN_IDENT)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TOKEN_EQ); err != nil {
			return nil, err
		}
		val, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, Assignment{Column: colTok.Literal, Value: val})
		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	if _, err := p.expect(TOKEN_WHERE); err != nil {
		return nil, err
	}
	where, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	p.match(TOKEN_SEMICOLON)

	return &UpdateStmt{
		TableName:   nameTok.Literal,
		Assignments: assignments,
		Where:       where,
	}, nil
}

// parseDelete parses: DELETE FROM ident WHERE expr ;?
func (p *parser) parseDelete() (Statement, error) {
	if _, err := p.expect(TOKEN_DELETE); err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_FROM); err != nil {
		return nil, err
	}
	nameTok, err := p.expect(TOKEN_IDENT)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TOKEN_WHERE); err != nil {
		return nil, err
	}
	where, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	p.match(TOKEN_SEMICOLON)

	return &DeleteStmt{
		TableName: nameTok.Literal,
		Where:     where,
	}, nil
}

// parseExpr parses: compare_expr (AND|OR compare_expr)*
func (p *parser) parseExpr() (Expr, error) {
	left, err := p.parseCompare()
	if err != nil {
		return nil, err
	}

	for p.check(TOKEN_AND) || p.check(TOKEN_OR) {
		opTok := p.advance()
		right, err := p.parseCompare()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{
			Left:  left,
			Op:    strings.ToUpper(opTok.Literal),
			Right: right,
		}
	}

	return left, nil
}

// parseCompare parses: primary (=|!=|<|>|<=|>=) primary | primary
func (p *parser) parseCompare() (Expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	tok := p.peek()
	switch tok.Type {
	case TOKEN_EQ, TOKEN_NEQ, TOKEN_LT, TOKEN_GT, TOKEN_LTE, TOKEN_GTE:
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Op: tok.Literal, Right: right}, nil
	}

	return left, nil
}

// parsePrimary parses: literal | ident | ( expr )
func (p *parser) parsePrimary() (Expr, error) {
	tok := p.peek()
	switch tok.Type {
	case TOKEN_NUMBER:
		p.advance()
		n, err := strconv.ParseInt(tok.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number %q at position %d: %w", tok.Literal, tok.Pos, err)
		}
		return &Literal{Value: n}, nil

	case TOKEN_STRING:
		p.advance()
		return &Literal{Value: tok.Literal}, nil

	case TOKEN_TRUE:
		p.advance()
		return &Literal{Value: true}, nil

	case TOKEN_FALSE:
		p.advance()
		return &Literal{Value: false}, nil

	case TOKEN_NULL:
		p.advance()
		return &Literal{Value: nil}, nil

	case TOKEN_IDENT:
		p.advance()
		return &ColumnRef{Name: tok.Literal}, nil

	case TOKEN_LPAREN:
		p.advance()
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TOKEN_RPAREN); err != nil {
			return nil, err
		}
		return expr, nil

	default:
		return nil, fmt.Errorf("unexpected token %s (%q) at position %d in expression", tok.Type, tok.Literal, tok.Pos)
	}
}
