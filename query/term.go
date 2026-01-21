package query

import (
	"fmt"
	"math"
	"simpledbgo/record"
	"simpledbgo/types"
)

type Term struct {
	lhs, rhs *Expression
}

func NewTerm(lhs, rhs *Expression) *Term {
	return &Term{lhs: lhs, rhs: rhs}
}

func (t *Term) IsSatisfied(scan types.Scan) bool {
	lhsval := t.lhs.Evaluate(scan)
	rhsval := t.rhs.Evaluate(scan)
	return lhsval == rhsval
}

func (t *Term) AppliedTo(schema *record.Schema) bool {
	return t.lhs.AppliedTo(schema) && t.rhs.AppliedTo(schema)
}

func (t *Term) ReductionFactor(p types.Plan) int {
	var lhsName string
	var rhsName string

	if t.lhs.isFieldName() && t.rhs.isFieldName() {
		lhsName = t.lhs.AsFieldName()
		rhsName = t.rhs.AsFieldName()
		return max(p.DistinctValues(lhsName), p.DistinctValues(rhsName))
	}

	if t.lhs.isFieldName() {
		lhsName = t.lhs.AsFieldName()
		return p.DistinctValues(lhsName)
	}
	if t.rhs.isFieldName() {
		rhsName = t.rhs.AsFieldName()
		return p.DistinctValues(rhsName)
	}

	// otherwise, the term equates constants
	if types.ConstantEqual(t.lhs.AsConstant(), t.rhs.AsConstant()) {
		return 1
	}
	return math.MaxInt32
}

func (t *Term) EquatesWithConstant(fieldName string) *types.Constant {
	if t.lhs.isFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.isFieldName() {
		return t.rhs.AsConstant()
	}

	if t.rhs.isFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.isFieldName() {
		return t.lhs.AsConstant()
	}

	return nil
}

func (t *Term) EquatesWithField(fieldName string) *string {
	if t.lhs.isFieldName() && t.lhs.AsFieldName() == fieldName && t.rhs.isFieldName() {
		s := t.rhs.AsFieldName()
		return &s
	}

	if t.rhs.isFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.isFieldName() {
		s := t.lhs.AsFieldName()
		return &s
	}

	return nil
}

func (t Term) String() string {
	return fmt.Sprintf("<TERM(%v,%v)>", t.lhs, t.rhs)
}

func (t *Term) AsString() string {
	return t.lhs.AsString() + " = " + t.rhs.AsString()
}
