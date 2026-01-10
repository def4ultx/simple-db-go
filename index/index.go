package index

import (
	"simpledbgo/query"
	"simpledbgo/record"
)

type Index interface {
	BeforeFirst(searchKey *query.Constant)
	Next() bool
	GetDataRowID() record.RowID
	Insert(dataVal *query.Constant, rowID record.RowID)
	Delete(dataVal *query.Constant, rowID record.RowID)
	Close()
}
