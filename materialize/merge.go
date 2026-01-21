package materialize

import (
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type MergeJoinPlan struct {
	p1         types.Plan
	p2         types.Plan
	fieldName1 string
	fieldName2 string
	schema     *record.Schema
}

func NewMergeJoinPlan(tx *tx.Transaction, p1, p2 types.Plan, field1, field2 string) *MergeJoinPlan {
	sp1 := NewSortPlan(p1, []string{field1}, tx)
	sp2 := NewSortPlan(p2, []string{field2}, tx)

	schema := record.NewSchema()
	schema.AddAll(sp1.Schema())
	schema.AddAll(sp2.Schema())

	plan := &MergeJoinPlan{
		p1:         sp1,
		p2:         sp2,
		fieldName1: field1,
		fieldName2: field2,
		schema:     schema,
	}
	return plan
}

func (t *MergeJoinPlan) Open() types.Scan {
	s1 := t.p1.Open()
	s2 := t.p2.Open().(*SortScan)
	return NewMergeJoinScan(s1, s2, t.fieldName1, t.fieldName2)
}

func (t *MergeJoinPlan) BlocksAccessed() int {
	return t.p1.BlocksAccessed() + t.p2.BlocksAccessed()
}

func (t *MergeJoinPlan) RecordsOutput() int {
	maxValue := max(t.p1.DistinctValues(t.fieldName1), t.p2.DistinctValues(t.fieldName2))
	return t.p1.BlocksAccessed() * t.p2.BlocksAccessed() / maxValue
}

func (t *MergeJoinPlan) DistinctValues(fieldName string) int {
	if t.p1.Schema().HasField(fieldName) {
		return t.p1.DistinctValues(fieldName)
	} else {
		return t.p2.DistinctValues(fieldName)
	}
}

func (t *MergeJoinPlan) Schema() *record.Schema {
	return t.schema
}

type MergeJoinScan struct {
	s1      types.Scan
	s2      *SortScan
	field1  string
	field2  string
	joinVal *types.Constant
}

func NewMergeJoinScan(s1 types.Scan, s2 *SortScan, field1, field2 string) *MergeJoinScan {
	t := &MergeJoinScan{
		s1:     s1,
		s2:     s2,
		field1: field1,
		field2: field2,
	}
	t.BeforeFirst()
	return t
}

func (t *MergeJoinScan) BeforeFirst() {
	t.s1.BeforeFirst()
	t.s2.BeforeFirst()
}

func (t *MergeJoinScan) Next() bool {
	hasMore2 := t.s2.Next()
	if hasMore2 && types.ConstantEqual(t.s2.GetVal(t.field2), t.joinVal) {
		return true
	}

	hasMore1 := t.s1.Next()
	if hasMore1 && types.ConstantEqual(t.s1.GetVal(t.field1), t.joinVal) {
		t.s2.restorePosition()
		return true
	}

	for hasMore1 && hasMore2 {
		v1 := t.s1.GetVal(t.field1)
		v2 := t.s2.GetVal(t.field2)

		r := types.ConstantCompareTo(v1, v2)
		if r < 0 {
			hasMore1 = t.s1.Next()
		} else if r > 0 {
			hasMore2 = t.s2.Next()
		} else {
			t.s2.savePosition()
			t.joinVal = t.s2.GetVal(t.field2)
			return true
		}
	}
	return false
}

func (t *MergeJoinScan) Close() {
	t.s1.Close()
	t.s2.Close()
}

func (t *MergeJoinScan) GetInt(fieldName string) int {
	if t.s1.HasField(fieldName) {
		return t.s1.GetInt(fieldName)
	} else {
		return t.s2.GetInt(fieldName)
	}
}

func (t *MergeJoinScan) GetString(fieldName string) string {
	if t.s1.HasField(fieldName) {
		return t.s1.GetString(fieldName)
	} else {
		return t.s2.GetString(fieldName)
	}
}

func (t *MergeJoinScan) GetVal(fieldName string) *types.Constant {
	if t.s1.HasField(fieldName) {
		return t.s1.GetVal(fieldName)
	} else {
		return t.s2.GetVal(fieldName)
	}
}

func (t *MergeJoinScan) HasField(fieldName string) bool {
	return t.s1.HasField(fieldName) || t.s2.HasField(fieldName)
}
