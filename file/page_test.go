package file

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPage(t *testing.T) {
	t.Run("get and set int", func(t *testing.T) {
		table := []int{
			0,
			1,
			100,
			1234,
			100000,
			math.MaxInt32,
			math.MinInt32,
		}

		for _, v := range table {
			p := NewPage(32)

			p.SetInt(0, v)

			actual := p.GetInt(0)
			assert.Equal(t, actual, v)
		}
	})
}
