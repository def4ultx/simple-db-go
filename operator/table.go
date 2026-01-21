package operator

import (
	"simpledbgo/file"
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type TableScan struct {
	tx          *tx.Transaction
	layout      *record.Layout
	rp          *record.RecordPage
	filename    string
	currentSlot int
}

func NewTableScan(tx *tx.Transaction, tableName string, layout *record.Layout) *TableScan {
	filename := tableName + ".tbl"
	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		filename: filename,
	}

	if tx.Size(filename) == 0 {
		ts.moveToNewBlock()
	} else {
		ts.moveToBlock(0)
	}
	return ts
}

func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.BlockID())
	}
}

func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

func (ts *TableScan) Next() bool {
	ts.currentSlot = ts.rp.NextAfter(ts.currentSlot)
	for ts.currentSlot < 0 {
		if ts.atLastBlock() {
			return false
		}

		ts.moveToBlock(ts.rp.BlockID().BlockNumber)
		ts.currentSlot = ts.rp.NextAfter(ts.currentSlot)
	}
	return true
}

func (ts *TableScan) GetInt(fieldName string) int {
	return ts.rp.GetInt(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetString(fieldName string) string {
	return ts.rp.GetString(ts.currentSlot, fieldName)
}

func (ts *TableScan) GetVal(fieldName string) *types.Constant {
	if ts.layout.Schema().Type(fieldName) == record.FieldTypeInteger {
		return types.NewIntConstant(ts.GetInt(fieldName))
	} else {
		return types.NewStringConstant(ts.GetString(fieldName))
	}
}

func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.Schema().HasField(fieldName)
}

func (ts *TableScan) SetInt(fieldName string, val int) {
	ts.rp.SetInt(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetString(fieldName string, val string) {
	ts.rp.SetString(ts.currentSlot, fieldName, val)
}

func (ts *TableScan) SetVal(fieldName string, val *types.Constant) {
	if ts.layout.Schema().Type(fieldName) == record.FieldTypeInteger {
		ts.SetInt(fieldName, val.AsInt())
	} else {
		ts.SetString(fieldName, val.AsString())
	}
}

func (ts *TableScan) Insert() {
	ts.currentSlot = ts.rp.InsertAfter(ts.currentSlot)
	for ts.currentSlot < 0 {
		if ts.atLastBlock() {
			ts.moveToNewBlock()
		} else {
			ts.moveToBlock(ts.rp.BlockID().BlockNumber + 1)
		}
		ts.currentSlot = ts.rp.InsertAfter(ts.currentSlot)
	}
}

func (ts *TableScan) Delete() {
	ts.rp.Delete(ts.currentSlot)
}

func (ts *TableScan) MoveToRowID(rid record.RowID) {
	ts.Close()
	block := &file.BlockID{
		Filename:    ts.filename,
		BlockNumber: rid.BlockNumber(),
	}
	ts.rp = record.NewRecordPage(ts.tx, block, ts.layout)
	ts.currentSlot = rid.Slot()
}

func (ts *TableScan) GetRowID() record.RowID {
	return record.NewRowID(ts.rp.BlockID().BlockNumber, ts.currentSlot)
}

func (ts *TableScan) moveToBlock(blockNum int) {
	ts.Close()
	block := ts.tx.Append(ts.filename)
	ts.rp = record.NewRecordPage(ts.tx, block, ts.layout)
	ts.currentSlot = -1
}

func (ts *TableScan) moveToNewBlock() {
	ts.Close()
	block := ts.tx.Append(ts.filename)
	ts.rp = record.NewRecordPage(ts.tx, block, ts.layout)
	ts.rp.Format()
	ts.currentSlot = -1
}

func (ts *TableScan) atLastBlock() bool {
	return ts.rp.BlockID().BlockNumber == ts.tx.Size(ts.filename)-1
}
