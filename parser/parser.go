package parser

import (
	"simpledbgo/query"
	"simpledbgo/record"
)

/*
Grammar

<Field> := IdTok
<Constant> := StrTok | IntTok
<Expression> := <Field> | <Constant>
<Term> := <Expression> = <Expression>
<Predicate> := <Term> [ AND <Predicate> ]
<Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
<SelectList> := <Field> [ , <SelectList> ]
<TableList> := IdTok [ , <TableList> ]
<UpdateCmd> <Create> <Insert> <FieldList> <ConstList> := <Insert> | <Delete> | <Modify> | <Create>
:= <CreateTable> | <CreateView> | <CreateIndex>
:= INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )
:= <Field> [ , <FieldList> ]
:= <Constant> [ , <ConstList> ]
<Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]
<Modify> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]
<CreateTable> := CREATE TABLE IdTok ( <FieldDefs> )
<FieldDefs> := <FieldDef> [ , <FieldDefs> ]
<FieldDef> := IdTok <TypeDef>
<TypeDef> := INT | VARCHAR ( IntTok )
<CreateView> := CREATE VIEW IdTok AS <Query>
<CreateIndex> := CREATE INDEX IdTok ON IdTok ( <Field> )
*/

type PredicateParser struct {
	lexer *Lexer
}

func NewPredicateParser(source string) *PredicateParser {
	lx := NewLexer(source)
	pp := &PredicateParser{
		lexer: lx,
	}
	return pp
}

func (pp *PredicateParser) Field() string {
	return pp.lexer.eatID()
}

func (pp *PredicateParser) Constant() *query.Constant {
	if pp.lexer.matchStringConstant() {
		return query.NewStringConstant(pp.lexer.eatStringConstant())
	} else {
		return query.NewIntConstant(pp.lexer.eatIntConstant())
	}
}

func (pp *PredicateParser) Expression() *query.Expression {
	if pp.lexer.matchID() {
		return query.NewFieldExpression(pp.Field())
	} else {
		return query.NewConstantExpression(pp.Constant())
	}
}

func (pp *PredicateParser) Term() *query.Term {
	lhs := pp.Expression()
	pp.lexer.eatDelim('=')
	rhs := pp.Expression()
	return query.NewTerm(lhs, rhs)
}

func (pp *PredicateParser) Predicate() *query.Predicate {
	pred := query.NewPredicateWithTerm(pp.Term())
	if pp.lexer.matchKeyword("and") {
		pp.lexer.eatKeyword("and")
		pred.ConjoinWith(pp.Predicate())
	}
	return pred
}

// Parsing queries

func (pp *PredicateParser) Query() *QueryData {
	pp.lexer.eatKeyword("select")
	fields := pp.selectList()
	pp.lexer.eatKeyword("from")
	tables := pp.tableList()

	pred := query.NewPredicate()
	if pp.lexer.matchKeyword("where") {
		pp.lexer.eatKeyword("where")
		pred = pp.Predicate()
	}

	qd := &QueryData{
		Fields:    fields,
		Tables:    tables,
		Predicate: pred,
	}
	return qd
}

func (pp *PredicateParser) selectList() []string {
	l := make([]string, 0)

	l = append(l, pp.Field())
	if pp.lexer.matchDelim(',') {
		pp.lexer.eatDelim(',')
		l = append(l, pp.selectList()...)
	}
	return l
}

func (pp *PredicateParser) tableList() []string {
	l := make([]string, 0)

	l = append(l, pp.lexer.eatID())
	if pp.lexer.matchDelim(',') {
		pp.lexer.eatDelim(',')
		l = append(l, pp.tableList()...)
	}
	return l
}

func (pp *PredicateParser) Command() any {
	switch {
	case pp.lexer.matchKeyword("insert"):
		return pp.insert()
	case pp.lexer.matchKeyword("delete"):
		return pp.delete()
	case pp.lexer.matchKeyword("update"):
		return pp.update()
	default:
		return pp.create()
	}
}

func (pp *PredicateParser) create() any {
	pp.lexer.eatKeyword("create")
	if pp.lexer.matchKeyword("table") {
		return pp.createTable()
	} else if pp.lexer.matchKeyword("view") {
		return pp.createView()
	} else {
		return pp.createIndex()
	}
}

func (pp *PredicateParser) delete() *DeleteData {
	pp.lexer.eatKeyword("delete")
	pp.lexer.eatKeyword("from")
	tableName := pp.lexer.eatID()
	pred := query.NewPredicate()

	if pp.lexer.matchKeyword("where") {
		pp.lexer.eatKeyword("where")

		pred = pp.Predicate()
	}

	dd := &DeleteData{
		TableName: tableName,
		Predicate: pred,
	}
	return dd
}

func (pp *PredicateParser) insert() *InsertData {
	pp.lexer.eatKeyword("insert")
	pp.lexer.eatKeyword("into")

	tableName := pp.lexer.eatID()
	pp.lexer.eatDelim('(')
	fields := pp.fieldList()
	pp.lexer.eatDelim('(')

	pp.lexer.eatKeyword("values")
	pp.lexer.eatDelim('(')
	values := pp.constList()
	pp.lexer.eatDelim('(')

	id := &InsertData{
		TableName: tableName,
		Fields:    fields,
		Values:    values,
	}
	return id
}

func (pp *PredicateParser) fieldList() []string {
	l := make([]string, 0)
	l = append(l, pp.Field())

	if pp.lexer.matchDelim(',') {
		pp.lexer.eatDelim(',')
		l = append(l, pp.fieldList()...)
	}
	return l
}

func (pp *PredicateParser) constList() []*query.Constant {
	l := make([]*query.Constant, 0)
	l = append(l, pp.Constant())

	if pp.lexer.matchDelim(',') {
		pp.lexer.eatDelim(',')
		l = append(l, pp.constList()...)
	}
	return l
}

func (pp *PredicateParser) update() *UpdateData {
	pp.lexer.eatKeyword("update")
	tableName := pp.lexer.eatID()

	pp.lexer.eatKeyword("set")
	fieldName := pp.Field()
	pp.lexer.eatDelim('=')
	newValue := pp.Expression()

	pred := query.NewPredicate()
	if pp.lexer.matchKeyword("where") {
		pp.lexer.eatKeyword("where")
		pred = pp.Predicate()
	}

	ud := &UpdateData{
		TableName: tableName,
		FieldName: fieldName,
		NewValue:  newValue,
		Predicate: pred,
	}
	return ud
}

func (pp *PredicateParser) createTable() *CreateTableData {
	pp.lexer.eatKeyword("table")
	tableName := pp.lexer.eatID()
	pp.lexer.eatDelim('(')

	schema := pp.fieldDefs()
	pp.lexer.eatDelim(')')

	ctd := &CreateTableData{
		TableName: tableName,
		Schema:    schema,
	}
	return ctd
}

func (pp *PredicateParser) fieldDefs() *record.Schema {
	schema := pp.fieldDef()
	if pp.lexer.matchDelim(',') {
		pp.lexer.eatDelim(',')
		sch := pp.fieldDefs()
		schema.AddAll(sch)
	}
	return schema
}

func (pp *PredicateParser) fieldDef() *record.Schema {
	fieldName := pp.Field()
	return pp.fieldType(fieldName)
}

func (pp *PredicateParser) fieldType(fieldName string) *record.Schema {
	schema := record.NewSchema()
	if pp.lexer.matchKeyword("int") {
		pp.lexer.eatKeyword("int")
		schema.AddIntField(fieldName)
	} else {
		pp.lexer.eatKeyword("varchar")
		pp.lexer.eatDelim('(')
		length := pp.lexer.eatIntConstant()
		pp.lexer.eatDelim(')')
		schema.AddStringFiled(fieldName, length)
	}
	return schema
}

func (pp *PredicateParser) createView() *CreateViewData {
	pp.lexer.eatKeyword("view")
	viewName := pp.lexer.eatID()
	pp.lexer.eatKeyword("as")
	qd := pp.Query()

	cvd := &CreateViewData{
		ViewName:  viewName,
		QueryData: qd,
	}
	return cvd
}

func (pp *PredicateParser) createIndex() *CreateIndexData {
	pp.lexer.eatKeyword("index")
	idxName := pp.lexer.eatID()
	pp.lexer.eatKeyword("on")
	tblName := pp.lexer.eatID()
	pp.lexer.eatDelim('(')
	fldName := pp.Field()
	pp.lexer.eatDelim(')')

	cid := &CreateIndexData{
		IndexName: idxName,
		TableName: tblName,
		FieldName: fldName,
	}
	return cid
}

type QueryData struct {
	Fields    []string
	Tables    []string
	Predicate *query.Predicate
}

func (q *QueryData) AsString() string {
	var result string

	result = "select "

	for _, v := range q.Fields {
		result += v + ", "
	}
	result = result[:len(result)-2] // zap final comma

	result += " from "

	for _, v := range q.Tables {
		result += v + ", "
	}
	result = result[:len(result)-2] // zap final comma

	pred := q.Predicate.AsString()
	if pred != "" {
		result += " where " + pred
	}
	return result
}

type InsertData struct {
	TableName string
	Fields    []string
	Values    []*query.Constant
}

type DeleteData struct {
	TableName string
	Predicate *query.Predicate
}

type UpdateData struct {
	TableName string
	FieldName string
	NewValue  *query.Expression
	Predicate *query.Predicate
}

type CreateTableData struct {
	TableName string
	Schema    *record.Schema
}

type CreateViewData struct {
	ViewName  string
	QueryData *QueryData
}

func (c *CreateViewData) ViewDef() string {
	return c.QueryData.AsString()
}

type CreateIndexData struct {
	IndexName string
	TableName string
	FieldName string
}
