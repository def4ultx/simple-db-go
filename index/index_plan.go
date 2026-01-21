package index

import (
	"simpledbgo/operator"
	"simpledbgo/record"
	"simpledbgo/types"
)

type IndexSelectPlan struct {
	plan types.Plan
	ii   *IndexInfo
	val  *types.Constant
}

func NewIndexSelectPlan(p types.Plan, ii *IndexInfo, val *types.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{
		plan: p,
		ii:   ii,
		val:  val,
	}
}

func (s *IndexSelectPlan) Open() types.Scan {
	// throw exception if p is not a table plan
	ts := s.plan.Open().(*operator.TableScan)
	idx := s.ii.Open()
	return NewIndexSelectScan(ts, idx, s.val)
}

func (s *IndexSelectPlan) BlocksAccessed() int {
	return s.ii.BlocksAccessed() + s.RecordsOutput()
}

func (s *IndexSelectPlan) RecordsOutput() int {
	return s.ii.RecordsOutput()
}

func (s *IndexSelectPlan) DistinctValues(fieldName string) int {
	return s.ii.DistinctValues(fieldName)
}

func (s *IndexSelectPlan) Schema() *record.Schema {
	return s.plan.Schema()
}

type IndexSelectScan struct {
	ts    *operator.TableScan
	index types.Index
	val   *types.Constant
}

func NewIndexSelectScan(ts *operator.TableScan, idx types.Index, val *types.Constant) types.Scan {
	scan := &IndexSelectScan{
		ts:    ts,
		index: idx,
		val:   val,
	}
	scan.BeforeFirst()
	return scan
}

func (s *IndexSelectScan) BeforeFirst() {
	s.index.BeforeFirst(s.val)
}

func (s *IndexSelectScan) Next() bool {
	ok := s.index.Next()
	if ok {
		rid := s.index.GetDataRowID()
		s.ts.MoveToRowID(rid)
	}
	return ok
}

func (s *IndexSelectScan) GetInt(fieldName string) int {
	return s.ts.GetInt(fieldName)
}

func (s *IndexSelectScan) GetString(fieldName string) string {
	return s.ts.GetString(fieldName)
}

func (s *IndexSelectScan) GetVal(fieldName string) *types.Constant {
	return s.ts.GetVal(fieldName)
}

func (s *IndexSelectScan) HasField(fieldName string) bool {
	return s.ts.HasField(fieldName)
}

func (s *IndexSelectScan) Close() {
	s.index.Close()
	s.ts.Close()
}

type IndexJoinPlan struct {
	p1        types.Plan
	p2        types.Plan
	ii        *IndexInfo
	joinField string
	schema    *record.Schema
}

func NewIndexJoinPlan(p1, p2 types.Plan, ii *IndexInfo, joinField string) *IndexJoinPlan {
	ijp := &IndexJoinPlan{
		p1:        p1,
		p2:        p2,
		ii:        ii,
		joinField: joinField,
		schema:    record.NewSchema(),
	}
	ijp.schema.AddAll(p1.Schema())
	ijp.schema.AddAll(p2.Schema())
	return ijp
}

func (p *IndexJoinPlan) Open() types.Scan {
	s := p.p1.Open()
	// throw exception if p2 is not a table plan
	ts := p.p2.Open().(*operator.TableScan)
	idx := p.ii.Open()
	return NewIndexJoinScan(s, idx, p.joinField, ts)
}

func (p *IndexJoinPlan) BlocksAccessed() int {
	return p.p1.BlocksAccessed() + (p.p1.RecordsOutput() * p.ii.BlocksAccessed()) + p.RecordsOutput()
}

func (p *IndexJoinPlan) RecordsOutput() int {
	return p.p1.RecordsOutput() * p.ii.RecordsOutput()
}

func (p *IndexJoinPlan) DistinctValues(fieldName string) int {
	if p.p1.Schema().HasField(fieldName) {
		return p.p1.DistinctValues(fieldName)
	} else {
		return p.p2.DistinctValues(fieldName)
	}
}

func (p *IndexJoinPlan) Schema() *record.Schema {
	return p.schema
}

type IndexJoinScan struct {
	lhs       types.Scan
	index     types.Index
	joinField string
	rhs       *operator.TableScan
}

func NewIndexJoinScan(lhs types.Scan, idx types.Index, joinField string, rhs *operator.TableScan) *IndexJoinScan {
	plan := &IndexJoinScan{
		lhs:       lhs,
		index:     idx,
		joinField: joinField,
		rhs:       rhs,
	}
	plan.BeforeFirst()
	return plan
}

func (p *IndexJoinScan) BeforeFirst() {
	p.lhs.BeforeFirst()
	p.lhs.Next()
	p.resetIndex()
}

func (p *IndexJoinScan) Next() bool {
	for {
		if p.index.Next() {
			p.rhs.MoveToRowID(p.index.GetDataRowID())
			return true
		}
		if !p.lhs.Next() {
			return false
		}
		p.resetIndex()
	}
}

func (p *IndexJoinScan) GetInt(fieldName string) int {
	if p.rhs.HasField(fieldName) {
		return p.rhs.GetInt(fieldName)
	} else {
		return p.lhs.GetInt(fieldName)
	}
}
func (p *IndexJoinScan) GetString(fieldName string) string {
	if p.rhs.HasField(fieldName) {
		return p.rhs.GetString(fieldName)
	} else {
		return p.lhs.GetString(fieldName)
	}
}

func (p *IndexJoinScan) GetVal(fieldName string) *types.Constant {
	if p.rhs.HasField(fieldName) {
		return p.rhs.GetVal(fieldName)
	} else {
		return p.lhs.GetVal(fieldName)
	}
}

func (p *IndexJoinScan) HasField(fieldName string) bool {
	return p.rhs.HasField(fieldName) || p.lhs.HasField(fieldName)
}

func (p *IndexJoinScan) Close() {
	p.lhs.Close()
	p.index.Close()
	p.rhs.Close()
}

func (p *IndexJoinScan) resetIndex() {
	searchKey := p.lhs.GetVal(p.joinField)
	p.index.BeforeFirst(searchKey)
}
