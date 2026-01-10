package tx

import (
	"simpledbgo/buffer"
	"simpledbgo/log"
	"slices"
)

type RecoveryManager struct {
	lm    *log.Manager
	bm    *buffer.Manager
	tx    *Transaction
	txNum int
}

func NewRecoveryManager(tx *Transaction, txNum int, lm *log.Manager, bm *buffer.Manager) *RecoveryManager {
	mgr := &RecoveryManager{
		lm:    lm,
		bm:    bm,
		tx:    tx,
		txNum: txNum,
	}

	RecordStartWriteToLog(lm, txNum)
	return mgr
}

func (mgr *RecoveryManager) Commit() {
	mgr.bm.FlushAll(mgr.txNum)
	lsn := RecordCommitWriteToLog(mgr.lm, mgr.txNum)
	mgr.lm.Flush(lsn)
}

func (mgr *RecoveryManager) Rollback() {
	mgr.doRollback()
	mgr.bm.FlushAll(mgr.txNum)

	lsn := RecordRollbackWriteToLog(mgr.lm, mgr.txNum)
	mgr.lm.Flush(lsn)
}

func (mgr *RecoveryManager) Recover() {
	mgr.doRecover()
	mgr.bm.FlushAll(mgr.txNum)

	lsn := RecordCheckpointWriteToLog(mgr.lm)
	mgr.lm.Flush(lsn)
}

func (mgr *RecoveryManager) SetInt(buf *buffer.Buffer, offset int, newVal int) int {
	oldVal := buf.Page().GetInt(offset)
	blockID := buf.BlockID()

	return RecordSetIntWriteToLog(mgr.lm, mgr.txNum, blockID, offset, oldVal)
}

func (mgr *RecoveryManager) SetString(buf *buffer.Buffer, offset int, newVal string) int {
	oldVal := buf.Page().GetString(offset)
	blockID := buf.BlockID()

	return RecordSetStringWriteToLog(mgr.lm, mgr.txNum, blockID, offset, oldVal)
}

func (mgr *RecoveryManager) doRollback() {
	iter := mgr.lm.Iterator()
	for iter.HasNext() {
		b := iter.Next()
		rec := CreateRecord(b)

		if rec.TxNumber() == mgr.txNum {
			if rec.Op() == RecordTypeStart {
				return
			}
			rec.Undo(mgr.tx)
		}
	}
}

func (mgr *RecoveryManager) doRecover() {
	finishedTxs := make([]int, 0)
	iter := mgr.lm.Iterator()
	for iter.HasNext() {
		b := iter.Next()
		rec := CreateRecord(b)

		if rec.Op() == RecordTypeCheckpoint {
			return
		}

		if rec.Op() == RecordTypeCommit || rec.Op() == RecordTypeRollback {
			finishedTxs = append(finishedTxs, rec.TxNumber())
		} else if !slices.Contains(finishedTxs, rec.TxNumber()) {
			rec.Undo(mgr.tx)
		}
	}
}
