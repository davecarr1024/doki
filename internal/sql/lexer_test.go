package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLex_Keywords(t *testing.T) {
	tests := []struct {
		input    string
		wantType TokenType
	}{
		{"SELECT", TOKEN_SELECT},
		{"select", TOKEN_SELECT},
		{"Select", TOKEN_SELECT},
		{"INSERT", TOKEN_INSERT},
		{"insert", TOKEN_INSERT},
		{"UPDATE", TOKEN_UPDATE},
		{"DELETE", TOKEN_DELETE},
		{"CREATE", TOKEN_CREATE},
		{"TABLE", TOKEN_TABLE},
		{"FROM", TOKEN_FROM},
		{"WHERE", TOKEN_WHERE},
		{"INTO", TOKEN_INTO},
		{"VALUES", TOKEN_VALUES},
		{"SET", TOKEN_SET},
		{"PRIMARY", TOKEN_PRIMARY},
		{"KEY", TOKEN_KEY},
		{"INT", TOKEN_INT},
		{"TEXT", TOKEN_TEXT},
		{"BOOL", TOKEN_BOOL},
		{"NOT", TOKEN_NOT},
		{"NULL", TOKEN_NULL},
		{"AND", TOKEN_AND},
		{"OR", TOKEN_OR},
		{"TRUE", TOKEN_TRUE},
		{"FALSE", TOKEN_FALSE},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			tokens, err := Lex(tc.input)
			require.NoError(t, err)
			require.Len(t, tokens, 2) // keyword + EOF
			assert.Equal(t, tc.wantType, tokens[0].Type)
		})
	}
}

func TestLex_Identifiers(t *testing.T) {
	tokens, err := Lex("my_table")
	require.NoError(t, err)
	require.Len(t, tokens, 2)
	assert.Equal(t, TOKEN_IDENT, tokens[0].Type)
	assert.Equal(t, "my_table", tokens[0].Literal)

	tokens, err = Lex("col1")
	require.NoError(t, err)
	require.Len(t, tokens, 2)
	assert.Equal(t, TOKEN_IDENT, tokens[0].Type)
	assert.Equal(t, "col1", tokens[0].Literal)

	tokens, err = Lex("_underscore")
	require.NoError(t, err)
	require.Len(t, tokens, 2)
	assert.Equal(t, TOKEN_IDENT, tokens[0].Type)
	assert.Equal(t, "_underscore", tokens[0].Literal)
}

func TestLex_Numbers(t *testing.T) {
	tokens, err := Lex("42")
	require.NoError(t, err)
	require.Len(t, tokens, 2)
	assert.Equal(t, TOKEN_NUMBER, tokens[0].Type)
	assert.Equal(t, "42", tokens[0].Literal)

	tokens, err = Lex("0")
	require.NoError(t, err)
	require.Len(t, tokens, 2)
	assert.Equal(t, TOKEN_NUMBER, tokens[0].Type)
	assert.Equal(t, "0", tokens[0].Literal)
}

func TestLex_StringLiterals(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		tokens, err := Lex("'hello'")
		require.NoError(t, err)
		require.Len(t, tokens, 2)
		assert.Equal(t, TOKEN_STRING, tokens[0].Type)
		assert.Equal(t, "hello", tokens[0].Literal)
	})

	t.Run("embedded single quote", func(t *testing.T) {
		tokens, err := Lex("'it''s'")
		require.NoError(t, err)
		require.Len(t, tokens, 2)
		assert.Equal(t, TOKEN_STRING, tokens[0].Type)
		assert.Equal(t, "it's", tokens[0].Literal)
	})

	t.Run("empty string", func(t *testing.T) {
		tokens, err := Lex("''")
		require.NoError(t, err)
		require.Len(t, tokens, 2)
		assert.Equal(t, TOKEN_STRING, tokens[0].Type)
		assert.Equal(t, "", tokens[0].Literal)
	})

	t.Run("unterminated string", func(t *testing.T) {
		_, err := Lex("'unterminated")
		assert.Error(t, err)
	})
}

func TestLex_Punctuation(t *testing.T) {
	tests := []struct {
		input    string
		wantType TokenType
		wantLit  string
	}{
		{"(", TOKEN_LPAREN, "("},
		{")", TOKEN_RPAREN, ")"},
		{",", TOKEN_COMMA, ","},
		{";", TOKEN_SEMICOLON, ";"},
		{"=", TOKEN_EQ, "="},
		{"!=", TOKEN_NEQ, "!="},
		{"<", TOKEN_LT, "<"},
		{">", TOKEN_GT, ">"},
		{"<=", TOKEN_LTE, "<="},
		{">=", TOKEN_GTE, ">="},
		{"*", TOKEN_STAR, "*"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			tokens, err := Lex(tc.input)
			require.NoError(t, err)
			require.Len(t, tokens, 2)
			assert.Equal(t, tc.wantType, tokens[0].Type)
			assert.Equal(t, tc.wantLit, tokens[0].Literal)
		})
	}
}

func TestLex_WhitespaceSkipping(t *testing.T) {
	tokens, err := Lex("  SELECT   FROM  ")
	require.NoError(t, err)
	require.Len(t, tokens, 3) // SELECT, FROM, EOF
	assert.Equal(t, TOKEN_SELECT, tokens[0].Type)
	assert.Equal(t, TOKEN_FROM, tokens[1].Type)
}

func TestLex_CommentSkipping(t *testing.T) {
	tokens, err := Lex("SELECT -- this is a comment\nFROM")
	require.NoError(t, err)
	require.Len(t, tokens, 3) // SELECT, FROM, EOF
	assert.Equal(t, TOKEN_SELECT, tokens[0].Type)
	assert.Equal(t, TOKEN_FROM, tokens[1].Type)
}

func TestLex_EOF(t *testing.T) {
	tokens, err := Lex("")
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	assert.Equal(t, TOKEN_EOF, tokens[0].Type)
}

func TestLex_ErrorOnUnknownCharacter(t *testing.T) {
	_, err := Lex("@")
	assert.Error(t, err)

	_, err = Lex("SELECT # bad")
	assert.Error(t, err)
}

func TestLex_MultipleTokens(t *testing.T) {
	input := "SELECT id, name FROM users WHERE id = 42"
	tokens, err := Lex(input)
	require.NoError(t, err)

	expected := []TokenType{
		TOKEN_SELECT, TOKEN_IDENT, TOKEN_COMMA, TOKEN_IDENT,
		TOKEN_FROM, TOKEN_IDENT, TOKEN_WHERE, TOKEN_IDENT,
		TOKEN_EQ, TOKEN_NUMBER, TOKEN_EOF,
	}
	require.Len(t, tokens, len(expected))
	for i, tt := range expected {
		assert.Equal(t, tt, tokens[i].Type, "token %d", i)
	}
}

func TestLex_Position(t *testing.T) {
	tokens, err := Lex("abc 123")
	require.NoError(t, err)
	assert.Equal(t, 0, tokens[0].Pos)
	assert.Equal(t, 4, tokens[1].Pos)
}
