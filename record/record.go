package record

import (
	"simpledbgo/file"
	"simpledbgo/tx"
)

const (
	FlagEmpty = 0
	FlagUsed  = 1
)

type RecordPage struct {
	tx     *tx.Transaction
	block  *file.BlockID
	layout *Layout
}

func NewRecordPage(tx *tx.Transaction, block *file.BlockID, layout *Layout) *RecordPage {
	tx.Pin(block)

	r := &RecordPage{
		tx:     tx,
		block:  block,
		layout: layout,
	}
	return r
}

func (r *RecordPage) GetInt(slot int, fieldName string) int {
	pos := r.offset(slot) + r.layout.Offset(fieldName)
	return r.tx.GetInt(r.block, pos)
}

func (r *RecordPage) GetString(slot int, fieldName string) string {
	pos := r.offset(slot) + r.layout.Offset(fieldName)
	return r.tx.GetString(r.block, pos)
}

func (r *RecordPage) SetInt(slot int, fieldName string, val int) {
	pos := r.offset(slot) + r.layout.Offset(fieldName)
	r.tx.SetInt(r.block, pos, val, true)
}

func (r *RecordPage) SetString(slot int, fieldName string, val string) {
	pos := r.offset(slot) + r.layout.Offset(fieldName)
	r.tx.SetString(r.block, pos, val, true)
}

func (r *RecordPage) Delete(slot int) {
	r.setFlag(slot, FlagEmpty)
}

func (r *RecordPage) Format() {
	slot := 0
	for r.isValidSlot(slot) {
		r.tx.SetInt(r.block, r.offset(slot), FlagEmpty, false)

		sch := r.layout.Schema()
		for _, f := range sch.Fields() {
			pos := r.offset(slot) + r.layout.Offset(f)
			if sch.Type(f) == FieldTypeInteger {
				r.tx.SetInt(r.block, pos, 0, false)
			} else {
				r.tx.SetString(r.block, pos, "", false)
			}
		}

		slot++
	}
}

func (r *RecordPage) NextAfter(slot int) int {
	return r.searchAfter(slot, FlagUsed)
}

func (r *RecordPage) InsertAfter(slot int) int {
	newSlot := r.searchAfter(slot, FlagEmpty)
	if newSlot >= 0 {
		r.setFlag(newSlot, FlagUsed)
	}
	return newSlot
}

func (r *RecordPage) BlockID() *file.BlockID {
	return r.block
}

func (r *RecordPage) setFlag(slot int, flag int) {
	r.tx.SetInt(r.block, r.offset(slot), flag, true)
}

func (r *RecordPage) searchAfter(slot int, flag int) int {
	slot++
	for r.isValidSlot(slot) {
		if r.tx.GetInt(r.block, r.offset(slot)) == flag {
			return slot
		}
		slot++
	}
	return -1
}

func (r *RecordPage) isValidSlot(slot int) bool {
	return r.offset(slot+1) <= r.tx.BlockSize()
}

func (r *RecordPage) offset(slot int) int {
	return slot * r.layout.slotSize
}
