package sql

import (
	"fmt"
	"strings"
)

// Lex tokenizes the input SQL string and returns a slice of tokens.
// It returns an error if an unrecognized character is encountered.
func Lex(input string) ([]Token, error) {
	l := &lexer{input: input, pos: 0}
	return l.lex()
}

type lexer struct {
	input string
	pos   int
}

func (l *lexer) peek() (byte, bool) {
	if l.pos >= len(l.input) {
		return 0, false
	}
	return l.input[l.pos], true
}

func (l *lexer) peekAt(offset int) (byte, bool) {
	idx := l.pos + offset
	if idx >= len(l.input) {
		return 0, false
	}
	return l.input[idx], true
}

func (l *lexer) advance() byte {
	ch := l.input[l.pos]
	l.pos++
	return ch
}

func (l *lexer) lex() ([]Token, error) {
	var tokens []Token

	for {
		l.skipWhitespaceAndComments()

		if l.pos >= len(l.input) {
			tokens = append(tokens, Token{Type: TOKEN_EOF, Literal: "", Pos: l.pos})
			break
		}

		startPos := l.pos
		ch, _ := l.peek()

		switch {
		case isLetter(ch):
			tok := l.lexIdentOrKeyword()
			tokens = append(tokens, tok)

		case isDigit(ch):
			tok := l.lexNumber()
			tokens = append(tokens, tok)

		case ch == '\'':
			tok, err := l.lexString()
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, tok)

		case ch == '(':
			l.advance()
			tokens = append(tokens, Token{Type: TOKEN_LPAREN, Literal: "(", Pos: startPos})

		case ch == ')':
			l.advance()
			tokens = append(tokens, Token{Type: TOKEN_RPAREN, Literal: ")", Pos: startPos})

		case ch == ',':
			l.advance()
			tokens = append(tokens, Token{Type: TOKEN_COMMA, Literal: ",", Pos: startPos})

		case ch == ';':
			l.advance()
			tokens = append(tokens, Token{Type: TOKEN_SEMICOLON, Literal: ";", Pos: startPos})

		case ch == '*':
			l.advance()
			tokens = append(tokens, Token{Type: TOKEN_STAR, Literal: "*", Pos: startPos})

		case ch == '=':
			l.advance()
			tokens = append(tokens, Token{Type: TOKEN_EQ, Literal: "=", Pos: startPos})

		case ch == '!':
			next, ok := l.peekAt(1)
			if ok && next == '=' {
				l.advance()
				l.advance()
				tokens = append(tokens, Token{Type: TOKEN_NEQ, Literal: "!=", Pos: startPos})
			} else {
				return nil, fmt.Errorf("unexpected character '!' at position %d", startPos)
			}

		case ch == '<':
			next, ok := l.peekAt(1)
			if ok && next == '=' {
				l.advance()
				l.advance()
				tokens = append(tokens, Token{Type: TOKEN_LTE, Literal: "<=", Pos: startPos})
			} else {
				l.advance()
				tokens = append(tokens, Token{Type: TOKEN_LT, Literal: "<", Pos: startPos})
			}

		case ch == '>':
			next, ok := l.peekAt(1)
			if ok && next == '=' {
				l.advance()
				l.advance()
				tokens = append(tokens, Token{Type: TOKEN_GTE, Literal: ">=", Pos: startPos})
			} else {
				l.advance()
				tokens = append(tokens, Token{Type: TOKEN_GT, Literal: ">", Pos: startPos})
			}

		default:
			return nil, fmt.Errorf("unexpected character %q at position %d", ch, startPos)
		}
	}

	return tokens, nil
}

// skipWhitespaceAndComments skips whitespace and -- line comments.
func (l *lexer) skipWhitespaceAndComments() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' {
			l.pos++
			continue
		}
		// Check for -- line comment
		if ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			// skip to end of line
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.pos++
			}
			continue
		}
		break
	}
}

// lexIdentOrKeyword scans an identifier or keyword.
func (l *lexer) lexIdentOrKeyword() Token {
	start := l.pos
	for l.pos < len(l.input) && isLetterOrDigit(l.input[l.pos]) {
		l.pos++
	}
	literal := l.input[start:l.pos]
	lower := strings.ToLower(literal)
	if tt, ok := keywords[lower]; ok {
		return Token{Type: tt, Literal: literal, Pos: start}
	}
	return Token{Type: TOKEN_IDENT, Literal: literal, Pos: start}
}

// lexNumber scans a sequence of digits.
func (l *lexer) lexNumber() Token {
	start := l.pos
	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.pos++
	}
	return Token{Type: TOKEN_NUMBER, Literal: l.input[start:l.pos], Pos: start}
}

// lexString scans a single-quoted string literal, handling '' as escaped quote.
func (l *lexer) lexString() (Token, error) {
	start := l.pos
	l.advance() // consume opening '
	var sb strings.Builder
	for {
		if l.pos >= len(l.input) {
			return Token{}, fmt.Errorf("unterminated string literal starting at position %d", start)
		}
		ch := l.advance()
		if ch == '\'' {
			// Check if next char is also ' (escaped quote)
			if l.pos < len(l.input) && l.input[l.pos] == '\'' {
				l.advance()
				sb.WriteByte('\'')
			} else {
				break
			}
		} else {
			sb.WriteByte(ch)
		}
	}
	return Token{Type: TOKEN_STRING, Literal: sb.String(), Pos: start}, nil
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetterOrDigit(ch byte) bool {
	return isLetter(ch) || isDigit(ch)
}
