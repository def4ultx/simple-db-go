package operator

import (
	"simpledbgo/query"
	"simpledbgo/record"
	"simpledbgo/types"
)

type SelectScan struct {
	scan      types.Scan
	predicate *query.Predicate
}

func NewSelectScan(scan types.Scan, predicate *query.Predicate) *SelectScan {
	ss := &SelectScan{
		scan:      scan,
		predicate: predicate,
	}
	return ss
}

func (s *SelectScan) BeforeFirst() {
	s.scan.BeforeFirst()
}

func (s *SelectScan) Next() bool {
	for s.scan.Next() {
		if s.predicate.IsSatisfied(s.scan) {
			return true
		}
	}
	return false
}

func (s *SelectScan) GetInt(fieldName string) int {
	return s.scan.GetInt(fieldName)
}

func (s *SelectScan) GetString(fieldName string) string {
	return s.scan.GetString(fieldName)
}

func (s *SelectScan) GetVal(fieldName string) *types.Constant {
	return s.scan.GetVal(fieldName)
}

func (s *SelectScan) HasField(fieldName string) bool {
	return s.HasField(fieldName)
}

func (s *SelectScan) Close() {
	s.scan.Close()
}

func (s *SelectScan) SetInt(fieldName string, val int) {
	us := s.scan.(types.UpdateScan)
	us.SetInt(fieldName, val)
}

func (s *SelectScan) SetString(fieldName string, val string) {
	us := s.scan.(types.UpdateScan)
	us.SetString(fieldName, val)
}

func (s *SelectScan) SetVal(fieldName string, val *types.Constant) {
	us := s.scan.(types.UpdateScan)
	us.SetVal(fieldName, val)
}

func (s *SelectScan) Insert() {
	us := s.scan.(types.UpdateScan)
	us.Insert()
}

func (s *SelectScan) Delete() {
	us := s.scan.(types.UpdateScan)
	us.Delete()
}

func (s *SelectScan) GetRowID() record.RowID {
	us := s.scan.(types.UpdateScan)
	return us.GetRowID()
}

func (s *SelectScan) MoveToRowID(rid record.RowID) {
	us := s.scan.(types.UpdateScan)
	us.MoveToRowID(rid)
}
