package log

import (
	"simpledbgo/file"
	"sync"
)

type Manager struct {
	fm      *file.Manager
	logfile string

	mu           sync.Mutex
	currentBlock *file.BlockID
	currentPage  *file.Page
	latestLSN    int
	lastSavedLSN int
}

func NewManager(fm *file.Manager, logfile string) *Manager {
	b := make([]byte, fm.BlockSize())
	p := file.NewPageFromBytes(b)

	logSize := fm.Length(logfile)

	var block *file.BlockID
	if logSize == 0 {
		block = fm.Append(logfile)
	} else {
		block := &file.BlockID{
			Filename:    logfile,
			BlockNumber: logSize - 1,
		}
		fm.Read(block, p)
	}

	mgr := &Manager{
		fm:           fm,
		logfile:      logfile,
		currentBlock: block,
		latestLSN:    0,
		lastSavedLSN: 0,
	}
	return mgr
}

func (mgr *Manager) Append(rec []byte) int {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	boundary := mgr.currentPage.GetInt(0)
	size := len(rec)

	intSize := 32 / 8

	bytesNeeded := size + intSize
	if boundary-bytesNeeded < intSize {
		// log record does not fit, move to next block
		mgr.flush()
		mgr.currentBlock = mgr.appendNewBlock()
		boundary = mgr.currentPage.GetInt(0)
	}

	recpos := boundary - bytesNeeded
	mgr.currentPage.SetBytes(recpos, rec)
	mgr.currentPage.SetInt(0, recpos)

	mgr.latestLSN++
	return mgr.latestLSN
}

func (mgr *Manager) appendNewBlock() *file.BlockID {
	block := mgr.fm.Append(mgr.logfile)
	mgr.currentPage.SetInt(0, mgr.fm.BlockSize())
	mgr.fm.Write(block, mgr.currentPage)
	return block
}

func (mgr *Manager) Flush(lsn int) {
	if lsn >= mgr.lastSavedLSN {
		mgr.flush()
	}
}

func (mgr *Manager) Iterator() *Iterator {
	mgr.flush()
	it := NewIterator(mgr.fm, mgr.currentBlock)
	return it
}

func (mgr *Manager) flush() {
	mgr.fm.Write(mgr.currentBlock, mgr.currentPage)
	mgr.lastSavedLSN = mgr.latestLSN
}
