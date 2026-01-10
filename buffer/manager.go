package buffer

import (
	"simpledbgo/file"
	"simpledbgo/log"
	"sync"
	"time"
)

type Manager struct {
	mu         sync.Mutex
	bufferPool []*Buffer
	available  int
}

func NewManager(fm *file.Manager, lm *log.Manager, n int) *Manager {
	pool := make([]*Buffer, n)
	for i := range pool {
		pool[i] = NewBuffer(fm, lm)
	}

	mgr := &Manager{
		bufferPool: pool,
		available:  n,
	}
	return mgr
}

func (mgr *Manager) Pin(blockID *file.BlockID) *Buffer {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	now := time.Now()
	buf := mgr.tryToPin(blockID)

	for buf == nil && !waitingTooLong(now) {
		buf = mgr.tryToPin(blockID)
	}

	if buf == nil {
		panic("cannot find a buffer")
	}
	return buf
}

func waitingTooLong(now time.Time) bool {
	return time.Now().After(now.Add(10 * time.Second))
}

func (mgr *Manager) tryToPin(blockID *file.BlockID) *Buffer {
	// Use existing buffer if exist
	for _, b := range mgr.bufferPool {
		if b.block == blockID {
			return b
		}
	}

	// Find unpinned buffer
	var buf *Buffer
	for _, b := range mgr.bufferPool {
		if b.isPinned() {
			continue
		}

		buf = b
		break
	}

	if !buf.isPinned() {
		mgr.available--
	}

	buf.assignToBlock(blockID)
	buf.pin()
	return buf
}

func (mgr *Manager) Unpin(buf *Buffer) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	buf.unpin()
	if !buf.isPinned() {
		mgr.available++
	}
}

func (mgr *Manager) NumAvailable() int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	return mgr.available
}

func (mgr *Manager) FlushAll(txNum int) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for _, b := range mgr.bufferPool {
		if b.ModifyingTx() == txNum {
			b.flush()
		}
	}
}
