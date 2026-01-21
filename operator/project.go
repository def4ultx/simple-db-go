package operator

import (
	"simpledbgo/types"
	"slices"
)

type ProjectScan struct {
	scan   types.Scan
	fields []string
}

func NewProjectScan(scan types.Scan, fields []string) *ProjectScan {
	ps := &ProjectScan{
		scan:   scan,
		fields: fields,
	}
	return ps
}

func (s *ProjectScan) BeforeFirst() {
	s.scan.BeforeFirst()
}

func (s *ProjectScan) Next() bool {
	return s.scan.Next()
}

func (s *ProjectScan) GetInt(fieldName string) int {
	if s.HasField(fieldName) {
		return s.scan.GetInt(fieldName)
	}
	panic("field not found")
}

func (s *ProjectScan) GetString(fieldName string) string {
	if s.HasField(fieldName) {
		return s.scan.GetString(fieldName)
	}
	panic("field not found")
}

func (s *ProjectScan) GetVal(fieldName string) *types.Constant {
	if s.HasField(fieldName) {
		return s.scan.GetVal(fieldName)
	}
	panic("field not found")
}

func (s *ProjectScan) HasField(fieldName string) bool {
	return slices.Contains(s.fields, fieldName)
}

func (s *ProjectScan) Close() {
	s.scan.Close()
}
