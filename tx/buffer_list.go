package tx

import (
	"simpledbgo/buffer"
	"simpledbgo/file"
	"slices"
)

type BufferList struct {
	buffers map[file.BlockID]*buffer.Buffer
	pins    []file.BlockID
	bm      *buffer.Manager
}

func NewBufferList(bm *buffer.Manager) *BufferList {
	b := &BufferList{
		buffers: make(map[file.BlockID]*buffer.Buffer),
		pins:    make([]file.BlockID, 0),
		bm:      bm,
	}
	return b
}

func (b *BufferList) GetBuffer(block *file.BlockID) *buffer.Buffer {
	return b.buffers[*block]
}

func (b *BufferList) Pin(block *file.BlockID) {
	buf := b.bm.Pin(block)
	b.buffers[*block] = buf
	b.pins = append(b.pins, *block)
}

func (b *BufferList) Unpin(block *file.BlockID) {
	buf := b.GetBuffer(block)
	b.bm.Unpin(buf)

	delete(b.buffers, *block)
	idx := slices.Index(b.pins, *block)
	if idx == -1 {
		return
	}

	slices.Delete(b.pins, idx, idx+1)
}

func (b *BufferList) UnpinAll() {
	for _, block := range b.pins {
		buf := b.GetBuffer(&block)
		b.bm.Unpin(buf)
	}

	b.buffers = make(map[file.BlockID]*buffer.Buffer)
	b.pins = make([]file.BlockID, 0)
}
