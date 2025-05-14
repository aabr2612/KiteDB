package graphdb

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/sirupsen/logrus"
)

// TokenType defines types of tokens
type TokenType int

const (
	TokenKeyword TokenType = iota
	TokenIdentifier
	TokenString
	TokenNumber
	TokenSymbol
	TokenEOF
)

// Token represents a lexical token
type Token struct {
	Type  TokenType
	Value string
}

// Tokenizer breaks a query into tokens
type Tokenizer struct {
	input  string
	pos    int
	tokens []Token
}

// NewTokenizer initializes a new Tokenizer
func NewTokenizer(input string) *Tokenizer {
	return &Tokenizer{
		input:  input,
		pos:    0,
		tokens: []Token{},
	}
}

// Tokenize processes the input query into tokens
func (t *Tokenizer) Tokenize() []Token {
	log := logrus.WithField("component", "Tokenizer")
	log.Debug("Starting tokenization")
	for t.pos < len(t.input) {
		switch {
		case unicode.IsSpace(rune(t.input[t.pos])):
			t.pos++
		case unicode.IsLetter(rune(t.input[t.pos])):
			t.readIdentifierOrKeyword()
		case t.input[t.pos] == '"':
			t.readString()
		case unicode.IsDigit(rune(t.input[t.pos])):
			t.readNumber()
		default:
			t.readSymbol()
		}
	}
	t.tokens = append(t.tokens, Token{Type: TokenEOF, Value: ""})
	log.WithField("token_count", len(t.tokens)).Info("Tokenization complete")
	// Debug: Log all tokens
	tokenList := make([]string, len(t.tokens))
	for i, token := range t.tokens {
		tokenList[i] = fmt.Sprintf("%v:%s", token.Type, token.Value)
	}
	log.WithField("tokens", strings.Join(tokenList, ", ")).Debug("Tokens produced")
	return t.tokens
}

// readIdentifierOrKeyword reads an identifier or keyword
func (t *Tokenizer) readIdentifierOrKeyword() {
	start := t.pos
	for t.pos < len(t.input) && (unicode.IsLetter(rune(t.input[t.pos])) || unicode.IsDigit(rune(t.input[t.pos])) || t.input[t.pos] == '_') {
		t.pos++
	}
	value := t.input[start:t.pos]
	upperValue := strings.ToUpper(value)
	tokenType := TokenIdentifier
	if upperValue == "CREATE" || upperValue == "MATCH" || upperValue == "SET" || upperValue == "DELETE" || upperValue == "RETURN" || upperValue == "WHERE" {
		tokenType = TokenKeyword
	}
	t.tokens = append(t.tokens, Token{Type: tokenType, Value: value})
}

// readString reads a quoted string
func (t *Tokenizer) readString() {
	t.pos++ // Skip opening quote
	start := t.pos
	for t.pos < len(t.input) && t.input[t.pos] != '"' {
		t.pos++
	}
	value := t.input[start:t.pos]
	t.pos++ // Skip closing quote
	t.tokens = append(t.tokens, Token{Type: TokenString, Value: value})
}

// readNumber reads a number
func (t *Tokenizer) readNumber() {
	start := t.pos
	for t.pos < len(t.input) && unicode.IsDigit(rune(t.input[t.pos])) {
		t.pos++
	}
	value := t.input[start:t.pos]
	t.tokens = append(t.tokens, Token{Type: TokenNumber, Value: value})
}

// readSymbol reads a symbol or operator
func (t *Tokenizer) readSymbol() {
	switch t.input[t.pos] {
	case '(':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "("})
		t.pos++
	case ')':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: ")"})
		t.pos++
	case '{':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "{"})
		t.pos++
	case '}':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "}"})
		t.pos++
	case ':':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: ":"})
		t.pos++
	case ',':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: ","})
		t.pos++
	case '=':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "="})
		t.pos++
	case '-':
		if t.pos+2 < len(t.input) && t.input[t.pos+1] == '>' {
			t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "->"})
			t.pos += 2
		} else {
			t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "-"})
			t.pos++
		}
	case '[':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "["})
		t.pos++
	case ']':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "]"})
		t.pos++
	case '.':
		t.tokens = append(t.tokens, Token{Type: TokenSymbol, Value: "."})
		t.pos++
	default:
		logrus.WithField("char", string(t.input[t.pos])).Warn("Unknown symbol, skipping")
		t.pos++
	}
}
