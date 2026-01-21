package materialize

import (
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
	"slices"
)

type GroupByPlan struct {
	plan         types.Plan
	fields       []string
	aggregateFns []AggregateFn
	schema       *record.Schema
}

func NewGroupByPlan(tx *tx.Transaction, plan types.Plan, fields []string, fns []AggregateFn) *GroupByPlan {
	p := NewSortPlan(plan, fields, tx)
	t := &GroupByPlan{
		plan:         p,
		fields:       fields,
		aggregateFns: fns,
		schema:       record.NewSchema(),
	}

	for _, field := range fields {
		t.schema.Add(field, p.schema)
	}
	for _, fn := range fns {
		t.schema.AddIntField(fn.FieldName())
	}

	return t
}

func (t *GroupByPlan) Open() types.Scan {
	s := t.plan.Open()
	return NewGroupByScan(s, t.fields, t.aggregateFns)
}

func (t *GroupByPlan) BlocksAccessed() int {
	return t.plan.BlocksAccessed()
}

func (t *GroupByPlan) RecordsOutput() int {
	numGroups := 1
	for _, field := range t.fields {
		numGroups *= t.plan.DistinctValues(field)
	}
	return numGroups
}

func (t *GroupByPlan) DistinctValues(fieldName string) int {
	if t.plan.Schema().HasField(fieldName) {
		return t.DistinctValues(fieldName)
	} else {
		return t.RecordsOutput()
	}
}

func (t *GroupByPlan) Schema() *record.Schema {
	return t.schema
}

type GroupByScan struct {
	scan         types.Scan
	fields       []string
	aggregateFns []AggregateFn
	groupVal     *GroupValue
	moreGroups   bool
}

func NewGroupByScan(scan types.Scan, fields []string, fns []AggregateFn) *GroupByScan {
	t := &GroupByScan{
		scan:         scan,
		fields:       fields,
		aggregateFns: fns,
	}
	t.BeforeFirst()
	return t
}

func (t *GroupByScan) BeforeFirst() {
	t.scan.BeforeFirst()
	t.moreGroups = t.scan.Next()
}

func (t *GroupByScan) Next() bool {
	if !t.moreGroups {
		return false
	}

	for _, fn := range t.aggregateFns {
		fn.ProcessFirst(t.scan)
	}
	t.groupVal = NewGroupVal(t.scan, t.fields)
	for {
		t.moreGroups = t.scan.Next()
		gv := NewGroupVal(t.scan, t.fields)
		if !GroupValEq(t.groupVal, gv) {
			break
		}

		for _, fn := range t.aggregateFns {
			fn.ProcessFirst(t.scan)
		}
	}
	return true
}

func (t *GroupByScan) Close() {
	t.scan.Close()
}

func (t *GroupByScan) GetInt(fieldName string) int {
	return t.GetVal(fieldName).AsInt()
}

func (t *GroupByScan) GetString(fieldName string) string {
	return t.GetVal(fieldName).AsString()
}

func (t *GroupByScan) GetVal(fieldName string) *types.Constant {
	if slices.Contains(t.fields, fieldName) {
		return t.groupVal.vals[fieldName]
	}

	for _, fn := range t.aggregateFns {
		if fn.FieldName() == fieldName {
			return fn.Value()
		}
	}

	panic("field not exist")
}

func (t *GroupByScan) HasField(fieldName string) bool {
	if slices.Contains(t.fields, fieldName) {
		return true
	}

	for _, fn := range t.aggregateFns {
		if fn.FieldName() == fieldName {
			return true
		}
	}

	return false
}

type GroupValue struct {
	vals map[string]*types.Constant
}

func NewGroupVal(scan types.Scan, fields []string) *GroupValue {
	m := make(map[string]*types.Constant)
	for _, field := range fields {
		m[field] = scan.GetVal(field)
	}
	gv := &GroupValue{
		vals: m,
	}
	return gv
}

func GroupValEq(a, b *GroupValue) bool {
	// TODO: Check len?
	// if len(a.vals) != len(b.vals) { return false }

	for field, v1 := range a.vals {
		v2, ok := b.vals[field]
		if !ok {
			return false
		}

		if !types.ConstantEqual(v1, v2) {
			return false
		}
	}

	return true
}

type AggregateFn interface {
	ProcessFirst(scan types.Scan)
	ProcessNext(scan types.Scan)
	FieldName() string
	Value() *types.Constant
}

type MaxFn struct {
	fieldName string
	value     *types.Constant
}

func NewMaxFn(fieldName string) *MaxFn {
	f := &MaxFn{
		fieldName: fieldName,
	}
	return f
}

func (f *MaxFn) ProcessFirst(scan types.Scan) {
	f.value = scan.GetVal(f.fieldName)
}

func (f *MaxFn) ProcessNext(scan types.Scan) {
	newVal := scan.GetVal(f.fieldName)
	if types.ConstantCompareTo(f.value, newVal) > 0 {
		f.value = newVal
	}
}

func (f *MaxFn) FieldName() string {
	return "maxof" + f.fieldName
}

func (f *MaxFn) Value() *types.Constant {
	return f.value
}
