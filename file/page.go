package file

type Page struct {
	data []byte
}

// TODO: Pool or Arena
func NewPage(blockSize int) *Page {
	return &Page{data: make([]byte, blockSize)}
}

func NewPageFromBytes(b []byte) *Page {
	return &Page{data: b}
}

// TODO: varint
// TODO: uint32 instead of int for value?
func (p *Page) GetInt(offset int) int {
	bb := p.data[offset : offset+4]

	var n int

	n |= int(bb[0])
	n |= int(bb[1]) << 8
	n |= int(bb[2]) << 16
	n |= int(bb[3]) << 24

	return n
}

func (p *Page) GetBytes(offset int) []byte {
	n := p.GetInt(offset)
	return p.data[offset+4 : n+offset+4]
}

func (p *Page) GetString(offset int) string {
	b := p.GetBytes(offset)
	return string(b)
}

func (p *Page) SetInt(offset int, val int) {
	p.data[offset+0] = byte(val)
	p.data[offset+1] = byte(val >> 8)
	p.data[offset+2] = byte(val >> 16)
	p.data[offset+3] = byte(val >> 24)
}

func (p *Page) SetBytes(offset int, val []byte) {
	n := len(val)

	p.SetInt(offset, n)
	copy(p.data[offset+4:n+offset+4], val)
}

func (p *Page) SetString(offset int, val string) {
	b := []byte(val)
	p.SetBytes(offset, b)
}

// func (p *Page) MaxLength(strlen int) int {
// 	return 0
// }

func PageMaxLength(strlen int) int {
	return 32/8 + strlen
}
