package pods

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractIndex(t *testing.T) {
	rc := newRunnerCollection("test", 0, 0, 0, 0, nil, nil, nil)
	i1, e1 := rc.extractIndex("test-1-whocares")
	assert.Nil(t, e1)
	assert.Equal(t, 1, i1)
	_, e2 := rc.extractIndex("whocares-1-whocares")
	assert.NotNil(t, e2)
	i3, e3 := rc.extractIndex("test-1000-whocares")
	assert.Nil(t, e3)
	assert.Equal(t, 1000, i3)
}
