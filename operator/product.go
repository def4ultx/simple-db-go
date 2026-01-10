package operator

import (
	"simpledbgo/query"
	"simpledbgo/types"
)

type ProductScan struct {
	s1, s2 types.Scan
}

func NewProductScan(s1, s2 types.Scan) *ProductScan {
	ps := &ProductScan{
		s1: s1,
		s2: s2,
	}
	s1.Next()
	return ps
}

func (s *ProductScan) BeforeFirst() {
	s.s1.BeforeFirst()
	s.s1.Next()
	s.s2.BeforeFirst()
}

func (s *ProductScan) Next() bool {
	if s.s2.Next() {
		return true
	}

	s.s2.BeforeFirst()
	return s.s2.Next() && s.s1.Next()
}

func (s *ProductScan) GetInt(fieldName string) int {
	if s.s1.HasField(fieldName) {
		return s.s1.GetInt(fieldName)
	}
	return s.s2.GetInt(fieldName)
}

func (s *ProductScan) GetString(fieldName string) string {
	if s.s1.HasField(fieldName) {
		return s.s1.GetString(fieldName)
	}
	return s.s2.GetString(fieldName)
}

func (s *ProductScan) GetVal(fieldName string) *query.Constant {
	if s.s1.HasField(fieldName) {
		return s.s1.GetVal(fieldName)
	}
	return s.s2.GetVal(fieldName)
}

func (s *ProductScan) HasField(fieldName string) bool {
	return s.s1.HasField(fieldName) || s.s2.HasField(fieldName)
}

func (s *ProductScan) Close() {
	s.s1.Close()
	s.s2.Close()
}
