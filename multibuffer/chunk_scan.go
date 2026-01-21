package multibuffer

import (
	"simpledbgo/file"
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type ChunkScan struct {
	buffs                                 []*record.RecordPage
	tx                                    *tx.Transaction
	filename                              string
	layout                                *record.Layout
	startBufNum, endBufNum, currentBufNum int
	rp                                    *record.RecordPage
	currentSlot                           int
}

func NewChunkScan(tx *tx.Transaction, filename string, layout *record.Layout, start, end int) *ChunkScan {
	cs := &ChunkScan{
		buffs:         make([]*record.RecordPage, 0),
		tx:            tx,
		filename:      filename,
		layout:        layout,
		startBufNum:   start,
		endBufNum:     end,
		currentBufNum: 0,
		currentSlot:   0,
	}

	for i := start; i <= end; i++ {
		block := &file.BlockID{
			Filename:    filename,
			BlockNumber: i,
		}
		cs.buffs = append(cs.buffs, record.NewRecordPage(tx, block, layout))
	}
	cs.MoveToBlock(start)
	return cs
}

func (cs *ChunkScan) BeforeFirst() {
	cs.MoveToBlock(cs.startBufNum)
}

func (cs *ChunkScan) Next() bool {
	cs.currentSlot = cs.rp.NextAfter(cs.currentSlot)
	for cs.currentSlot < 0 {
		if cs.currentBufNum == cs.endBufNum {
			return false
		}

		cs.MoveToBlock(cs.rp.BlockID().BlockNumber + 1)
		cs.currentSlot = cs.rp.NextAfter(cs.currentSlot)
	}
	return true
}

func (cs *ChunkScan) Close() {
	for _, v := range cs.buffs {
		cs.tx.Unpin(v.BlockID())
	}
}

func (cs *ChunkScan) GetInt(fieldName string) int {
	return cs.rp.GetInt(cs.currentSlot, fieldName)
}

func (cs *ChunkScan) GetString(fieldName string) string {
	return cs.rp.GetString(cs.currentSlot, fieldName)
}

func (cs *ChunkScan) GetVal(fieldName string) *types.Constant {
	if cs.layout.Schema().Type(fieldName) == record.FieldTypeInteger {
		return types.NewIntConstant(cs.GetInt(fieldName))
	} else {
		return types.NewStringConstant(cs.GetString(fieldName))
	}
}

func (cs *ChunkScan) HasField(fieldName string) bool {
	return cs.layout.Schema().HasField(fieldName)
}

func (cs *ChunkScan) MoveToBlock(blockNum int) {
	cs.currentBufNum = blockNum
	cs.rp = cs.buffs[cs.currentBufNum-cs.startBufNum]
	cs.currentSlot = -1
}
