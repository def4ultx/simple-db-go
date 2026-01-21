package log

import "simpledbgo/file"

type Iterator struct {
	fm    *file.Manager
	block *file.BlockID
	page  *file.Page

	pos      int
	boundary int
}

func NewIterator(fm *file.Manager, block *file.BlockID) *Iterator {
	page := file.NewPage(fm.BlockSize())
	it := &Iterator{
		fm:    fm,
		block: block,
		page:  page,
	}
	it.moveToBlock(block)
	return it
}

const IntegerSize = 32 / 8

func (it *Iterator) HasNext() bool {
	return it.pos < it.fm.BlockSize() || it.block.BlockNumber > 0
}

func (it *Iterator) Next() []byte {
	if it.pos == it.fm.BlockSize() {
		block := &file.BlockID{
			Filename:    it.block.Filename,
			BlockNumber: it.block.BlockNumber - 1,
		}
		it.moveToBlock(block)
	}
	rec := it.page.GetBytes(it.pos)
	it.pos += IntegerSize + len(rec)
	return rec
}

func (it *Iterator) moveToBlock(block *file.BlockID) {
	it.fm.Read(block, it.page)
	it.boundary = it.page.GetInt(0)
	it.pos = it.boundary
}
