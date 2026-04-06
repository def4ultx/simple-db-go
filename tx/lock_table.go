package tx

import (
	"simpledbgo/file"
	"sync"
	"time"
)

type LockTable struct {
	mu    sync.Mutex
	locks map[file.BlockID]int
}

func NewLockTable() *LockTable {
	return &LockTable{
		locks: make(map[file.BlockID]int),
	}
}

func (t *LockTable) SharedLock(blockID *file.BlockID) {

	now := time.Now()
	for t.hasExclusiveLock(blockID) && !waitingTooLong(now) {
		time.Sleep(1 * time.Millisecond)
	}

	if t.hasExclusiveLock(blockID) {
		panic("cannot obtain shared lock")
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.locks[*blockID]++
}

func (t *LockTable) ExclusiveLock(blockID *file.BlockID) {

	now := time.Now()
	for t.hasOtherSharedLock(blockID) && !waitingTooLong(now) {
		time.Sleep(1 * time.Millisecond)
	}

	if t.hasOtherSharedLock(blockID) {
		panic("cannot obtain exclusive lock")
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.locks[*blockID]--
}

func (t *LockTable) Unlock(blockID *file.BlockID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	n, ok := t.locks[*blockID]
	if !ok {
		return
	}

	if n > 1 {
		t.locks[*blockID]--
	} else {
		delete(t.locks, *blockID)
	}
}

func (t *LockTable) hasExclusiveLock(blockID *file.BlockID) bool   { return t.locks[*blockID] < 0 }
func (t *LockTable) hasOtherSharedLock(blockID *file.BlockID) bool { return t.locks[*blockID] > 1 }

func waitingTooLong(t time.Time) bool {
	return time.Now().After(t.Add(10 * time.Second))
}
