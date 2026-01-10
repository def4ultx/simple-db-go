package types

import (
	"simpledbgo/record"
)

type Plan interface {
	Open() Scan
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(fieldName string) int
	Schema() *record.Schema
}
