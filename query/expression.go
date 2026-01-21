package query

import (
	"fmt"
	"simpledbgo/record"
	"simpledbgo/types"
)

type Expression struct {
	value     *types.Constant
	fieldName string
}

func NewConstantExpression(value *types.Constant) *Expression {
	return &Expression{value: value}
}

func NewFieldExpression(fieldName string) *Expression {
	return &Expression{fieldName: fieldName}
}

func (e *Expression) isFieldName() bool {
	return len(e.fieldName) > 0
}

func (e *Expression) AsConstant() *types.Constant {
	return e.value
}

func (e *Expression) AsFieldName() string {
	return e.fieldName
}

func (e *Expression) Evaluate(scan types.Scan) *types.Constant {
	if e.value != nil {
		return e.value
	}
	return scan.GetVal(e.fieldName)
}

func (e *Expression) AppliedTo(schema *record.Schema) bool {
	if e.value != nil {
		return true
	}
	return schema.HasField(e.fieldName)
}

func (e Expression) String() string {
	return fmt.Sprintf("<EXPRESSION %v,%s>", e.value, e.fieldName)
}

func (e *Expression) AsString() string {
	if e.value != nil {
		return e.value.String()
	}
	return e.fieldName
}
