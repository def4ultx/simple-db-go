package tx

import (
	logger "log"
	"simpledbgo/buffer"
	"simpledbgo/file"
	"simpledbgo/log"
)

var nextTxNum = 0

func nextTxNumber() int {
	nextTxNum++
	return nextTxNum
}

const EndOfFile = -1

type Transaction struct {
	rm *RecoveryManager
	cm *ConcurrencyManager
	bm *buffer.Manager
	fm *file.Manager

	txNum   int
	buffers *BufferList
}

func NewTransaction(fm *file.Manager, lm *log.Manager, bm *buffer.Manager) *Transaction {
	t := &Transaction{
		cm: NewConcurrencyManager(),
		bm: bm,
		fm: fm,

		txNum:   nextTxNumber(),
		buffers: NewBufferList(bm),
	}

	t.rm = NewRecoveryManager(t, t.txNum, lm, bm)
	return t
}

func (t *Transaction) Commit() {
	t.rm.Commit()
	t.cm.Release()
	t.buffers.UnpinAll()
	logger.Println("Tx", t.txNum, "committed")
}

func (t *Transaction) Rollback() {
	t.rm.Rollback()
	t.cm.Release()
	t.buffers.UnpinAll()
	logger.Println("Tx", t.txNum, "rolled back")
}

func (t *Transaction) Recover() {
	t.bm.FlushAll(t.txNum)
	t.rm.Recover()
}

func (t *Transaction) Pin(block *file.BlockID) {
	t.buffers.Pin(block)
}

func (t *Transaction) Unpin(block *file.BlockID) {
	t.buffers.Unpin(block)
}

func (t *Transaction) GetInt(block *file.BlockID, offset int) int {
	t.cm.SharedLock(block)
	buf := t.buffers.GetBuffer(block)
	return buf.Page().GetInt(offset)
}

func (t *Transaction) GetString(block *file.BlockID, offset int) string {
	t.cm.SharedLock(block)
	buf := t.buffers.GetBuffer(block)
	return buf.Page().GetString(offset)
}

func (t *Transaction) SetInt(block *file.BlockID, offset int, val int, okToLog bool) {
	t.cm.ExclusiveLock(block)
	buf := t.buffers.GetBuffer(block)

	lsn := -1
	if okToLog {
		lsn = t.rm.SetInt(buf, offset, val)
	}

	p := buf.Page()
	p.SetInt(offset, val)
	buf.SetModified(t.txNum, lsn)
}

func (t *Transaction) SetString(block *file.BlockID, offset int, val string, okToLog bool) {
	t.cm.ExclusiveLock(block)
	buf := t.buffers.GetBuffer(block)

	lsn := -1
	if okToLog {
		lsn = t.rm.SetString(buf, offset, val)
	}

	p := buf.Page()
	p.SetString(offset, val)
	buf.SetModified(t.txNum, lsn)
}

func (t *Transaction) Size(filename string) int {
	dummy := &file.BlockID{
		Filename:    filename,
		BlockNumber: EndOfFile,
	}

	t.cm.SharedLock(dummy)
	return t.fm.Length(filename)
}

func (t *Transaction) Append(filename string) *file.BlockID {
	dummy := &file.BlockID{
		Filename:    filename,
		BlockNumber: EndOfFile,
	}

	t.cm.ExclusiveLock(dummy)
	return t.fm.Append(filename)
}

func (t *Transaction) BlockSize() int {
	return t.fm.BlockSize()
}

func (t *Transaction) AvailableBuffer() int {
	return t.bm.NumAvailable()
}
