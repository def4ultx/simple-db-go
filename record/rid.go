package record

import "fmt"

type RowID struct {
	block int
	slot  int
}

func NewRowID(block int, slot int) RowID {
	return RowID{block: block, slot: slot}
}

func (r RowID) String() string {
	return fmt.Sprintf("[ROW %d, %d]", r.block, r.slot)
}

func (r RowID) BlockNumber() int {
	return r.block
}

func (r RowID) Slot() int {
	return r.slot
}
