package types

import (
	"simpledbgo/query"
	"simpledbgo/record"
)

type Scan interface {
	BeforeFirst()
	Next() bool
	GetInt(fieldName string) int
	GetString(fieldName string) string
	GetVal(fieldName string) *query.Constant
	HasField(fieldName string) bool
	Close()
}

type UpdateScan interface {
	Scan
	SetInt(fieldName string, val int)
	SetString(fieldName string, val string)
	SetVal(fieldName string, val *query.Constant)
	Insert()
	Delete()
	GetRowID() record.RowID
	MoveToRowID(rid record.RowID)
}
