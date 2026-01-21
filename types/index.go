package types

import (
	"simpledbgo/record"
)

type Index interface {
	BeforeFirst(searchKey *Constant)
	Next() bool
	GetDataRowID() record.RowID
	Insert(dataVal *Constant, rowID record.RowID)
	Delete(dataVal *Constant, rowID record.RowID)
	Close()
}
