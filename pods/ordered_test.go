package pods

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testT struct {
	index int
}

func (tt *testT) SetIndex(value int) {
	tt.index = value
}

func TestOrderedHP(t *testing.T) {
	oc := newOrderedCollection[*testT]()
	for range 10 {
		oc.fill(&testT{index: 0})
	}
	for i := 1; i <= 10; i++ {
		tt, exists := oc.get(i)
		assert.True(t, exists)
		assert.Equal(t, i, tt.index)
	}
	oc.delete(4)
	assert.Equal(t, 10, oc.index)
	assert.Equal(t, 4, oc.holes.Keys()[0])

	oc.delete(7)
	oc.delete(8)
	oc.delete(9)
	oc.delete(10)
	assert.Equal(t, 6, oc.index)
	assert.Equal(t, 1, len(oc.holes.Keys()))
}
