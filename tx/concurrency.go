package tx

import "simpledbgo/file"

type ConcurrencyManager struct {
	lockTable *LockTable
	locks     map[file.BlockID]string
}

var lockTable = NewLockTable()

func NewConcurrencyManager() *ConcurrencyManager {
	mgr := &ConcurrencyManager{
		lockTable: lockTable,
		locks:     make(map[file.BlockID]string),
	}
	return mgr
}

func (mgr *ConcurrencyManager) SharedLock(blockID *file.BlockID) {
	_, ok := mgr.locks[*blockID]
	if ok {
		return
	}

	mgr.lockTable.SharedLock(blockID)
	mgr.locks[*blockID] = "S"
}

func (mgr *ConcurrencyManager) ExclusiveLock(blockID *file.BlockID) {
	lockType, lockTypeOk := mgr.locks[*blockID]
	hasExclusiveLock := lockTypeOk && lockType == "X"

	if !hasExclusiveLock {
		mgr.SharedLock(blockID)
		mgr.lockTable.ExclusiveLock(blockID)
		mgr.locks[*blockID] = "X"
	}
}

func (mgr *ConcurrencyManager) Release() {
	for block := range mgr.locks {
		mgr.lockTable.Unlock(&block)
	}
	mgr.locks = make(map[file.BlockID]string)
}
