package multibuffer

import "math"

var BufferNeeds = &bufferNeeds{}

type bufferNeeds struct{}

func (b *bufferNeeds) BestRoot(available, size int) int {
	avail := available - 2 // reserve a couple of buffers
	if avail <= 1 {
		return 1
	}

	k := math.MaxInt32
	i := 1.0
	for k > avail {
		i++
		k = int(math.Ceil(math.Pow(float64(size), 1/i)))
	}
	return k
}

func (b *bufferNeeds) BestFactor(available, size int) int {
	avail := available - 2 // reserve a couple of buffers
	if avail <= 1 {
		return 1
	}

	k := math.MaxInt32
	i := 1.0
	for k > avail {
		i++
		k = int(math.Ceil(float64(size) / i))
	}
	return k
}
