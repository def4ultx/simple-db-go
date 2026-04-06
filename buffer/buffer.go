package buffer

import (
	"simpledbgo/file"
	"simpledbgo/log"
)

type Buffer struct {
	fm *file.Manager
	lm *log.Manager

	pins  int
	txNum int
	lsn   int

	page  *file.Page
	block *file.BlockID
}

func NewBuffer(fm *file.Manager, lm *log.Manager) *Buffer {
	buf := &Buffer{
		fm:    fm,
		lm:    lm,
		pins:  0,
		txNum: -1,
		lsn:   -1,
		page:  file.NewPage(fm.BlockSize()),
	}
	return buf
}

func (b *Buffer) pin()           { b.pins++ }
func (b *Buffer) unpin()         { b.pins-- }
func (b *Buffer) isPinned() bool { return b.pins > 0 }

func (b *Buffer) Page() *file.Page       { return b.page }
func (b *Buffer) BlockID() *file.BlockID { return b.block }

func (b *Buffer) SetModified(txNum, lsn int) {
	b.txNum = txNum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) ModifyingTx() int {
	return b.txNum
}

func (b *Buffer) assignToBlock(block *file.BlockID) {
	b.flush()
	b.block = block
	b.fm.Read(block, b.page)
	b.pins = 0
}

func (b *Buffer) flush() {
	if b.txNum >= 0 {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.block, b.page)
		b.txNum = -1
	}
}
