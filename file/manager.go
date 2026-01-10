package file

import (
	"errors"
	"io"
	"os"
	"strings"
	"sync"
)

type Manager struct {
	directory string
	blockSize int
	isNew     bool

	mu    sync.Mutex
	files map[string]*os.File
}

func NewManager(directory string, blockSize int) *Manager {
	err := os.Mkdir(directory, os.ModePerm)
	if err != nil && !errors.Is(err, os.ErrExist) {
		panic(err)
	}
	isNew := true
	if errors.Is(err, os.ErrExist) {
		isNew = false
	}

	info, err := os.Stat(directory)
	if err != nil {
		panic(err)
	}

	if !info.IsDir() {
		panic("not a directory")
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		panic(err)
	}

	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "temp") {
			continue
		}

		err = os.Remove(directory + "/" + e.Name())
		if err != nil {
			panic(err)
		}
	}

	mgr := &Manager{
		directory: directory,
		blockSize: blockSize,
		isNew:     isNew,
		files:     make(map[string]*os.File),
	}
	return mgr
}

func (mgr *Manager) IsNew() bool { return mgr.isNew }

// TODO: Synchronized
func (mgr *Manager) Read(block *BlockID, p *Page) {
	file := mgr.getFile(block.Filename)

	offset := int64(block.BlockNumber * mgr.blockSize)
	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		panic(err)
	}

	n, err := file.Read(p.data)
	if err != nil {
		panic(err)
	}

	if n != len(p.data) {
		panic("file read not equal buffer size")
	}
}

func (mgr *Manager) Write(block *BlockID, p *Page) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	file := mgr.getFile(block.Filename)

	offset := int64(block.BlockNumber * mgr.blockSize)

	n, err := file.WriteAt(p.data, offset)
	if err != nil {
		panic(err)
	}

	if n != len(p.data) {
		panic("file write not equal buffer size")
	}
}

func (mgr *Manager) Append(filename string) *BlockID {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	blockNumber := mgr.Length(filename)

	b := make([]byte, mgr.blockSize)
	file := mgr.getFile(filename)

	_, err := file.Write(b)
	if err != nil {
		panic(err)
	}

	blockID := &BlockID{
		Filename:    filename,
		BlockNumber: blockNumber,
	}
	return blockID
}

func (mgr *Manager) BlockSize() int {
	return mgr.blockSize
}

func (mgr *Manager) Length(filename string) int {
	file := mgr.getFile(filename)
	info, err := file.Stat()
	if err != nil {
		panic(err)
	}

	return int(info.Size() / int64(mgr.blockSize))
}

func (mgr *Manager) getFile(filename string) *os.File {
	f, ok := mgr.files[filename]
	if ok {
		return f
	}

	f, err := os.OpenFile(mgr.directory+"/"+filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic("err")
	}

	mgr.files[filename] = f
	return f
}
