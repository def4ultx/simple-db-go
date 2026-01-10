package log

import "simpledbgo/file"

type Iterator struct {
	fm    *file.Manager
	block *file.BlockID
	page  *file.Page

	pos      int
	boundary int
}

func (it *Iterator) HasNext() bool {
	panic("implement me")
}

func (it *Iterator) Next() []byte {
	panic("implement me")
}
