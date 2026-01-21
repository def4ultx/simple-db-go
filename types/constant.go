package types

import (
	"simpledbgo/internal/hash"
	"strconv"
)

type Constant struct {
	intVal    *int
	stringVal *string
}

func NewIntConstant(v int) *Constant {
	return &Constant{intVal: &v}
}

func NewStringConstant(v string) *Constant {
	return &Constant{stringVal: &v}
}

func (c *Constant) AsInt() int       { return *c.intVal }
func (c *Constant) AsString() string { return *c.stringVal }

func (c Constant) String() string {
	if c.intVal != nil {
		return strconv.Itoa(*c.intVal)
	}
	if c.stringVal != nil {
		return *c.stringVal
	}
	panic("unexpected constant value")
}

func (c *Constant) HashKey() uint64 {
	if c.intVal != nil {
		return hash.HashInt(*c.intVal)
	}
	if c.stringVal != nil {
		return hash.HashKey([]byte(*c.stringVal))
	}
	panic("unexpected constant value")
}

// TODO: Need comparable function (compareTo, equals, hashCode)
func ConstantEqual(a, b *Constant) bool {
	if a.intVal != nil && b.intVal != nil {
		return *(a.intVal) == *(b.intVal)
	}

	if a.stringVal != nil && b.stringVal != nil {
		return *(a.stringVal) == *(b.stringVal)
	}

	return false
}

func ConstantCompareTo(a, b *Constant) int {
	if a.intVal != nil && b.intVal != nil {

		aa, bb := *(a.intVal), *(b.intVal)
		if aa < bb {
			return -1
		}
		if aa > bb {
			return 1
		}
		return 0
	}

	if a.stringVal != nil && b.stringVal != nil {

		aa, bb := *(a.stringVal), *(b.stringVal)
		if aa < bb {
			return -1
		}
		if aa > bb {
			return 1
		}
		return 0
	}

	return 0
}
