package sql

// TokenType represents the type of a lexical token.
type TokenType string

const (
	// Keywords
	TOKEN_SELECT    TokenType = "SELECT"
	TOKEN_INSERT    TokenType = "INSERT"
	TOKEN_UPDATE    TokenType = "UPDATE"
	TOKEN_DELETE    TokenType = "DELETE"
	TOKEN_CREATE    TokenType = "CREATE"
	TOKEN_TABLE     TokenType = "TABLE"
	TOKEN_FROM      TokenType = "FROM"
	TOKEN_WHERE     TokenType = "WHERE"
	TOKEN_INTO      TokenType = "INTO"
	TOKEN_VALUES    TokenType = "VALUES"
	TOKEN_SET       TokenType = "SET"
	TOKEN_PRIMARY   TokenType = "PRIMARY"
	TOKEN_KEY       TokenType = "KEY"
	TOKEN_INT       TokenType = "INT"
	TOKEN_TEXT      TokenType = "TEXT"
	TOKEN_BOOL      TokenType = "BOOL"
	TOKEN_NOT       TokenType = "NOT"
	TOKEN_NULL      TokenType = "NULL"
	TOKEN_AND       TokenType = "AND"
	TOKEN_OR        TokenType = "OR"
	TOKEN_TRUE      TokenType = "TRUE"
	TOKEN_FALSE     TokenType = "FALSE"

	// Identifiers and literals
	TOKEN_IDENT  TokenType = "IDENT"
	TOKEN_NUMBER TokenType = "NUMBER"
	TOKEN_STRING TokenType = "STRING"

	// Punctuation
	TOKEN_LPAREN    TokenType = "("
	TOKEN_RPAREN    TokenType = ")"
	TOKEN_COMMA     TokenType = ","
	TOKEN_SEMICOLON TokenType = ";"
	TOKEN_EQ        TokenType = "="
	TOKEN_NEQ       TokenType = "!="
	TOKEN_LT        TokenType = "<"
	TOKEN_GT        TokenType = ">"
	TOKEN_LTE       TokenType = "<="
	TOKEN_GTE       TokenType = ">="
	TOKEN_STAR      TokenType = "*"

	TOKEN_EOF TokenType = "EOF"
)

// keywords maps lowercase keyword strings to their token types.
var keywords = map[string]TokenType{
	"select":  TOKEN_SELECT,
	"insert":  TOKEN_INSERT,
	"update":  TOKEN_UPDATE,
	"delete":  TOKEN_DELETE,
	"create":  TOKEN_CREATE,
	"table":   TOKEN_TABLE,
	"from":    TOKEN_FROM,
	"where":   TOKEN_WHERE,
	"into":    TOKEN_INTO,
	"values":  TOKEN_VALUES,
	"set":     TOKEN_SET,
	"primary": TOKEN_PRIMARY,
	"key":     TOKEN_KEY,
	"int":     TOKEN_INT,
	"text":    TOKEN_TEXT,
	"bool":    TOKEN_BOOL,
	"not":     TOKEN_NOT,
	"null":    TOKEN_NULL,
	"and":     TOKEN_AND,
	"or":      TOKEN_OR,
	"true":    TOKEN_TRUE,
	"false":   TOKEN_FALSE,
}

// Token represents a single lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}
