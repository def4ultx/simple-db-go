package materialize

import (
	"simpledbgo/query"
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type RecordComparator struct {
	fields []string
}

func NewRecordComparator(fields []string) *RecordComparator {
	return &RecordComparator{fields: fields}
}

func (c *RecordComparator) Compare(s1, s2 types.Scan) int {
	for _, field := range c.fields {
		v1 := s1.GetVal(field)
		v2 := s2.GetVal(field)

		res := query.CompareTo(v1, v2)
		if res != 0 {
			return res
		}
	}
	return 0
}

type SortPlan struct {
	plan       types.Plan
	tx         *tx.Transaction
	schema     *record.Schema
	comparator *RecordComparator
}

func NewSortPlan(p types.Plan, fields []string, tx *tx.Transaction) *SortPlan {
	sp := &SortPlan{
		plan:       p,
		tx:         tx,
		schema:     p.Schema(),
		comparator: NewRecordComparator(fields),
	}
	return sp
}

func (p *SortPlan) Open() types.Scan {
	src := p.plan.Open()
	runs := p.splitIntoRuns(src)
	src.Close()

	for len(runs) > 2 {
		runs = p.doMergeIteration(runs)
	}
	return NewSortScan(runs, p.comparator)
}

func (p *SortPlan) BlocksAccessed() int {
	// does not include the one-time cost of sorting
	mp := NewMaterializePlan(p.tx, p.plan)
	return mp.BlocksAccessed()
}

func (p *SortPlan) RecordsOutput() int {
	return p.plan.RecordsOutput()
}

func (p *SortPlan) DistinctValues(fieldName string) int {
	return p.plan.DistinctValues(fieldName)
}

func (p *SortPlan) Schema() *record.Schema {
	return p.schema
}

func (p *SortPlan) splitIntoRuns(src types.Scan) []*TempTable {
	temps := make([]*TempTable, 0)
	src.BeforeFirst()

	if !src.Next() {
		return temps
	}
	currentTemp := NewTempTable(p.tx, p.schema)
	temps = append(temps, currentTemp)
	currentScan := currentTemp.Open()
	for p.copy(src, currentScan) {
		if p.comparator.Compare(src, currentScan) < 0 {
			// start a new run
			currentScan.Close()
			currentTemp = NewTempTable(p.tx, p.schema)
			temps = append(temps, currentTemp)
			currentScan = currentTemp.Open().(types.UpdateScan)
		}
	}
	currentScan.Close()
	return temps
}

func (p *SortPlan) doMergeIteration(runs []*TempTable) []*TempTable {
	// Currently using only 2 buffer
	// Consider multi buffer
	/*
		available := p.tx.AvailableBuffer()
		numBuff := multibuffer.BufferNeeds.BestRoot(available, len(runs))
		runsToMerges := make([]*TempTable, numBuff)
		for i := 0; i < numBuff; i++ {
			runsToMerges[i] = runs[0]
			runs = runs[1:]
		}
	*/

	result := make([]*TempTable, 0)
	for len(runs) > 1 {
		p1 := runs[0]
		p2 := runs[1]

		runs = runs[2:]
		result = append(result, p.mergeTwoRuns(p1, p2))
	}

	if len(runs) == 1 {
		result = append(result, runs[0])
	}
	return result
}

func (p *SortPlan) mergeTwoRuns(p1, p2 *TempTable) *TempTable {
	src1 := p1.Open()
	src2 := p2.Open()

	result := NewTempTable(p.tx, p.schema)
	dst := result.Open()

	hasMore1 := src1.Next()
	hasMore2 := src2.Next()

	for hasMore1 && hasMore2 {
		if p.comparator.Compare(src1, src2) < 0 {
			hasMore1 = p.copy(src1, dst)
		} else {
			hasMore2 = p.copy(src2, dst)
		}
	}

	if hasMore1 {
		for hasMore1 {
			hasMore1 = p.copy(src1, dst)
		}
	} else {
		for hasMore2 {
			hasMore2 = p.copy(src2, dst)
		}
	}

	src1.Close()
	src2.Close()
	dst.Close()
	return result
}

func (p *SortPlan) copy(src types.Scan, dst types.UpdateScan) bool {
	dst.Insert()
	for _, field := range p.schema.Fields() {
		dst.SetVal(field, src.GetVal(field))
	}
	return src.Next()
}

type SortScan struct {
	s1            types.UpdateScan
	s2            types.UpdateScan
	currentScan   types.UpdateScan
	comparator    *RecordComparator
	hasMore1      bool
	hasMore2      bool
	savedPosition []record.RowID
}

func NewSortScan(runs []*TempTable, comparator *RecordComparator) *SortScan {
	ss := &SortScan{
		comparator: comparator,
	}

	ss.s1 = runs[0].Open().(types.UpdateScan)
	ss.hasMore1 = ss.s1.Next()
	if len(runs) > 1 {
		ss.s2 = runs[0].Open().(types.UpdateScan)
		ss.hasMore2 = ss.s2.Next()
	}
	return ss
}

func (s *SortScan) BeforeFirst() {
	s.s1.BeforeFirst()
	s.hasMore1 = s.s1.Next()
	if s.s2 != nil {
		s.s2.BeforeFirst()
		s.hasMore2 = s.s2.Next()
	}
}

func (s *SortScan) Next() bool {
	if s.currentScan == s.s1 {
		s.hasMore1 = s.s1.Next()
	} else if s.currentScan == s.s2 {
		s.hasMore2 = s.s2.Next()
	}

	if !s.hasMore1 && !s.hasMore2 {
		return false
	} else if s.hasMore1 && s.hasMore2 {
		if s.comparator.Compare(s.s1, s.s2) < 0 {
			s.currentScan = s.s1
		} else {
			s.currentScan = s.s2
		}
	} else if s.hasMore1 {
		s.currentScan = s.s1
	} else {
		s.currentScan = s.s2
	}
	return true

}

func (s *SortScan) Close() {
	s.s1.Close()
	if s.s2 != nil {
		s.s2.Close()
	}
}

func (s *SortScan) GetInt(fieldName string) int {
	return s.currentScan.GetInt(fieldName)
}

func (s *SortScan) GetString(fieldName string) string {
	return s.currentScan.GetString(fieldName)
}
func (s *SortScan) GetVal(fieldName string) *query.Constant {
	return s.currentScan.GetVal(fieldName)
}

func (s *SortScan) HasField(fieldName string) bool {
	return s.currentScan.HasField(fieldName)
}

func (s *SortScan) savePosition() {
	rid1 := s.s1.GetRowID()
	rid2 := s.s2.GetRowID()
	s.savedPosition = []record.RowID{rid1, rid2}
}

func (s *SortScan) restorePosition() {
	rid1 := s.savedPosition[0]
	rid2 := s.savedPosition[1]

	s.s1.MoveToRowID(rid1)
	s.s2.MoveToRowID(rid2)
}
