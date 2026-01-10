package materialize

import (
	"simpledbgo/operator"
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
	"strconv"
	"sync"
)

var nextTableLock sync.Mutex
var nextTableNum = 0

func nextTableName() string {
	nextTableLock.Lock()
	defer nextTableLock.Unlock()

	nextTableNum++
	return "temp" + strconv.Itoa(nextTableNum)
}

type TempTable struct {
	tx        *tx.Transaction
	tableName string
	layout    *record.Layout
}

func NewTempTable(tx *tx.Transaction, schema *record.Schema) *TempTable {
	return &TempTable{
		tx:        tx,
		tableName: nextTableName(),
		layout:    record.NewLayout(schema),
	}
}

func (t *TempTable) Open() types.UpdateScan {
	return operator.NewTableScan(t.tx, t.tableName, t.layout)
}

func (t *TempTable) TableName() string {
	return t.tableName
}

func (t *TempTable) GetLayout() *record.Layout {
	return t.layout
}
