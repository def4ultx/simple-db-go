package record

import "simpledbgo/file"

type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

func NewLayout(schema *Schema) *Layout {
	offsets := make(map[string]int)

	pos := 32 / 8
	for _, f := range schema.Fields() {
		offsets[f] = pos

		var lengthInBytes int

		t := schema.Type(f)
		if t == FieldTypeInteger {
			lengthInBytes = 32 / 8
		} else {
			lengthInBytes = file.PageMaxLength(schema.Length(f))
		}

		pos += lengthInBytes
	}

	layout := &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: pos,
	}
	return layout
}

func NewLayoutWithOffset(schema *Schema, offsets map[string]int, slotSize int) *Layout {
	layout := &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
	return layout
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

func (l *Layout) Offset(fieldName string) int {
	return l.offsets[fieldName]
}

func (l *Layout) SlotSize() int {
	return l.slotSize
}
