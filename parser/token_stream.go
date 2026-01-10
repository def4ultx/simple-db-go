package parser

import (
	"strings"
)

type TokenStream struct {
	// internal for iteration
	idx    int
	tokens []Token

	// internal for parsing
	source string
	offset int
}

func NewTokenStream(source string) *TokenStream {

	// TODO: Is it possible to use bufio.Scanner type
	// input := strings.NewReader(source)
	// scanner := bufio.NewScanner(input)
	// scanner.Split(bufio.ScanWords)

	ts := &TokenStream{
		idx:    0,
		tokens: make([]Token, 0),
		source: source,
		offset: 0,
	}

	token, ok := ts.nextToken()
	for ok {
		ts.tokens = append(ts.tokens, token)
		token, ok = ts.nextToken()
	}

	return ts
}

func (ts *TokenStream) Peek() *Token {
	if ts.idx >= len(ts.tokens) {
		return nil
	}
	return &ts.tokens[ts.idx]
}

func (ts *TokenStream) Next() *Token {
	if ts.idx >= len(ts.tokens) {
		return nil
	}

	t := &ts.tokens[ts.idx]
	ts.idx++
	return t
}

func (ts *TokenStream) nextToken() (Token, bool) {
	ts.offset = ts.skipWhitespace(ts.offset)

	var token Token
	switch {
	case ts.offset >= len(ts.source):
		return Token{}, false

	case isIdentifierStart(ts.source[ts.offset]):
		token = ts.scanIdentifier(ts.offset)
		ts.offset = token.endOffset

	case isNumberStart(ts.source[ts.offset]):
		token = ts.scanNumber(ts.offset)
		ts.offset = token.endOffset

	case isSymbolStart(ts.source[ts.offset]):
		token = ts.scanSymbol(ts.offset)
		ts.offset = token.endOffset

	case isCharsStart(ts.source[ts.offset]):
		token = ts.scanChars(ts.offset, ts.source[ts.offset])
		ts.offset = token.endOffset
	}
	return token, true
}

func (ts *TokenStream) skipWhitespace(startOffset int) int {
	return ts.indexOfFirst(startOffset, isNotWhitespace)
}

func (ts *TokenStream) scanNumber(startOffset int) Token {
	var endOffset int
	if ts.source[startOffset] == '-' {
		endOffset = ts.indexOfFirst(startOffset+1, isNotDigit)
	} else {
		endOffset = ts.indexOfFirst(startOffset, isNotDigit)
	}

	if endOffset == len(ts.source) {
		return Token{
			value:     string(ts.source[startOffset:endOffset]),
			typ:       LiteralLong,
			endOffset: endOffset,
		}
	}
	isFloat := ts.source[endOffset] == '.'
	if isFloat {
		endOffset = ts.indexOfFirst(endOffset+1, isNotDigit)
	}

	var typ TokenType
	if isFloat {
		typ = LiteralDouble
	} else {
		typ = LiteralLong
	}
	token := Token{
		value:     string(ts.source[startOffset:endOffset]),
		typ:       typ,
		endOffset: endOffset,
	}
	return token
}

func (ts *TokenStream) scanIdentifier(startOffset int) Token {
	if ts.source[startOffset] == '`' {
		endOffset := ts.getOffsetUntilTerminatedChar('`', startOffset)
		return Token{
			value:     string(ts.source[startOffset:endOffset]),
			typ:       LiteralIdentifier,
			endOffset: endOffset,
		}
	}

	endOffset := ts.indexOfFirst(startOffset, isNotIdentifierPart)
	text := string(ts.source[startOffset:endOffset])
	var typ TokenType
	if isAmbiguousIdentifier(text) {
		typ = ts.processAmbiguousIdentifier(endOffset, text)
	} else {
		typ = getKeywordType(text)
	}

	return Token{
		value:     text,
		typ:       typ,
		endOffset: endOffset,
	}
}

func (ts *TokenStream) getOffsetUntilTerminatedChar(terminatedChar byte, startOffset int) int {
	offset := -1
	for i := startOffset; i < len(ts.source); i++ {
		if ts.source[i] == terminatedChar {
			offset = i
			break
		}
	}

	if offset == -1 {
		panic("unmatched terminated char")
	}
	return offset
}

func (ts *TokenStream) processAmbiguousIdentifier(startOffset int, text string) TokenType {
	skipOffset := ts.skipWhitespace(startOffset)

	if skipOffset == len(ts.source) {
		return LiteralIdentifier
	}

	return getKeywordType(string(ts.source[skipOffset : skipOffset+2]))
}

func (ts *TokenStream) scanSymbol(startOffset int) Token {
	endOffset := ts.indexOfFirst(startOffset, isNotSymbol)
	text := ts.source[startOffset:endOffset]

	var typ TokenType
	for {
		symbol, ok := getSymbolType(text)
		if ok {
			typ = symbol
			break
		}

		if len(text) == 0 {
			panic("unmatched symbol")
		}
		endOffset--
		text = ts.source[startOffset:endOffset]
	}

	token := Token{
		value:     string(ts.source[startOffset:endOffset]),
		typ:       typ,
		endOffset: endOffset,
	}
	return token
}

func (ts *TokenStream) scanChars(startOffset int, terminatedChar byte) Token {
	endOffset := ts.getOffsetUntilTerminatedChar(terminatedChar, startOffset+1)
	token := Token{
		value:     string(ts.source[startOffset:endOffset]),
		typ:       LiteralString,
		endOffset: endOffset + 1,
	}
	return token
}

func (ts *TokenStream) indexOfFirst(startOffset int, predicate func(byte) bool) int {
	for i := startOffset; i < len(ts.source); i++ {
		if predicate(ts.source[i]) {
			return i
		}
	}
	return len(ts.source)
}

func getKeywordType(text string) TokenType {
	keywordSet := map[TokenType]bool{
		KeywordSelect:   true,
		KeywordAs:       true,
		KeywordFrom:     true,
		KeywordWhere:    true,
		KeywordGroup:    true,
		KeywordBy:       true,
		KeywordHaving:   true,
		KeywordOrder:    true,
		KeywordAsc:      true,
		KeywordDesc:     true,
		KeywordCast:     true,
		KeywordMin:      true,
		KeywordMax:      true,
		KeywordAvg:      true,
		KeywordSum:      true,
		KeywordCount:    true,
		KeywordDistinct: true,
		KeywordOr:       true,
		KeywordAnd:      true,
	}

	tt := TokenType(strings.ToUpper(text))
	_, ok := keywordSet[tt]
	if !ok {
		return LiteralIdentifier
	}
	return tt
}

func getSymbolType(text string) (TokenType, bool) {
	symbolSet := map[TokenType]bool{
		SymbolPlus:         true,
		SymbolSub:          true,
		SymbolStar:         true,
		SymbolSlash:        true,
		SymbolEq:           true,
		SymbolBangEq:       true,
		SymbolGt:           true,
		SymbolGtEq:         true,
		SymbolLt:           true,
		SymbolLtEq:         true,
		SymbolLeftParen:    true,
		SymbolRightParen:   true,
		SymbolLeftBrace:    true,
		SymbolRightBrace:   true,
		SymbolLeftBracket:  true,
		SymbolRightBracket: true,
		SymbolComma:        true,
		SymbolDot:          true,
		SymbolDoubleDot:    true,
		SymbolSemicolon:    true,
		SymbolColon:        true,
		SymbolPercent:      true,
		SymbolAmp:          true,
		SymbolBar:          true,
	}

	symbol := TokenType(text)
	if _, ok := symbolSet[symbol]; !ok {
		return "", false
	}
	return symbol, true
}

func isAmbiguousIdentifier(text string) bool {
	return TokenType(text) == KeywordOrder || TokenType(text) == KeywordGroup
}

func isNotWhitespace(ch byte) bool {
	return !isWhitespace(ch)
}

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func isNotDigit(ch byte) bool {
	return !isDigit(ch)
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isNumberStart(ch byte) bool {
	return isDigit(ch) || '.' == ch
}

func isNotSymbol(ch byte) bool {
	return !isSymbolStart(ch)
}

func isSymbolStart(ch byte) bool {
	var allSymbol = []string{
		string(SymbolPlus),
		string(SymbolSub),
		string(SymbolStar),
		string(SymbolSlash),
		string(SymbolEq),
		string(SymbolBangEq),
		string(SymbolGt),
		string(SymbolGtEq),
		string(SymbolLt),
		string(SymbolLtEq),
		string(SymbolLeftParen),
		string(SymbolRightParen),
		string(SymbolLeftBrace),
		string(SymbolRightBrace),
		string(SymbolLeftBracket),
		string(SymbolRightBracket),
		string(SymbolComma),
		string(SymbolDot),
		string(SymbolDoubleDot),
		string(SymbolSemicolon),
		string(SymbolColon),
		string(SymbolPercent),
		string(SymbolAmp),
		string(SymbolBar),
	}

	str := strings.Join(allSymbol, "")

	symbolSet := make(map[byte]bool)
	for _, ch := range str {
		symbolSet[byte(ch)] = true
	}

	_, ok := symbolSet[ch]
	return ok
}

func isIdentifierStart(ch byte) bool {
	return isLetter(ch)
}

func isNotIdentifierPart(ch byte) bool {
	return !isIdentifierPart(ch)
}

func isIdentifierPart(ch byte) bool {
	return isLetter(ch) || isDigit(ch) || '_' == ch
}

func isCharsStart(ch byte) bool {
	return ch == '\'' || ch == '"'
}
