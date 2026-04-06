package parser

import (
	"strconv"
)

type Lexer struct {
	keywords  []string
	tokenizer *TokenStream
}

func NewLexer(source string) *Lexer {
	lx := &Lexer{
		keywords:  make([]string, 0),
		tokenizer: NewTokenStream(source),
	}
	return lx
}

func (lx *Lexer) matchDelim(delim byte) bool {
	v := lx.tokenizer.Peek().value
	return len(v) == 1 && delim == v[0]
}

func (lx *Lexer) matchIntConstant() bool {
	return lx.tokenizer.Peek().typ == LiteralLong
}

func (lx *Lexer) matchStringConstant() bool {
	return lx.tokenizer.Peek().typ == LiteralString
}

func (lx *Lexer) matchKeyword(w string) bool {
	return lx.tokenizer.Peek().value == w
}

func (lx *Lexer) matchID() bool {
	return lx.tokenizer.Peek().typ == LiteralIdentifier
}

func (lx *Lexer) eatDelim(d byte) {
	if !lx.matchDelim(d) {
		panic("not delimiter")
	}
	lx.tokenizer.Next()
}

func (lx *Lexer) eatIntConstant() int {
	if !lx.matchIntConstant() {
		panic("not int constant")
	}
	t := lx.tokenizer.Next()
	v, err := strconv.Atoi(t.value)
	if err != nil {
		panic(err)
	}
	return v
}

func (lx *Lexer) eatStringConstant() string {
	if !lx.matchStringConstant() {
		panic("not string constant")
	}

	t := lx.tokenizer.Next()
	return t.value
}

func (lx *Lexer) eatKeyword(w string) string {
	if !lx.matchKeyword(w) {
		panic("not keyword")
	}

	t := lx.tokenizer.Next()
	return t.value
}

func (lx *Lexer) eatID() string {
	if !lx.matchID() {
		panic("not keyword")
	}

	t := lx.tokenizer.Next()
	return t.value
}

var initKeywords = []string{
	"select",
	"from",
	"where",
	"and",
	"insert",
	"into",
	"values",
	"delete",
	"update",
	"set",
	"create",
	"table",
	"varchar",
	"int",
	"view",
	"as",
	"index",
	"on",
}
