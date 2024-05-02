package pods

import (
	"sync"

	"github.com/elliotchance/orderedmap/v2"
)

type Indexed interface {
	SetIndex(int)
}

type orderedCollection[T Indexed] struct {
	sync.RWMutex
	index int
	holes *orderedmap.OrderedMap[int, struct{}]
	m     map[int]T
}

func newOrderedCollection[T Indexed]() *orderedCollection[T] {
	return &orderedCollection[T]{
		holes: orderedmap.NewOrderedMap[int, struct{}](),
		m:     map[int]T{},
	}
}

func (o *orderedCollection[T]) delete(key int) {
	o.Lock()
	defer o.Unlock()
	if key == o.index {
		o.index--
	} else if key < o.index {
		o.holes.Set(key, struct{}{})
	}
	for {
		if _, exists := o.holes.Get(o.index); exists {
			o.holes.Delete(o.index)
			o.index--
			if o.index == 0 {
				break
			}
			continue
		}
		break
	}
}

func (o *orderedCollection[T]) fill(t T) {
	o.Lock()
	defer o.Unlock()
	keys := o.holes.Keys()
	for _, key := range keys {
		o.m[key] = t
		o.holes.Delete(key)
		t.SetIndex(key)
		return
	}
	o.index++
	o.m[o.index] = t
	t.SetIndex(o.index)
}

func (o *orderedCollection[T]) set(key int, t T) {
	o.Lock()
	defer o.Unlock()
	o.m[key] = t
	if key > o.index {
		for i := o.index; i < key; i++ {
			o.holes.Set(i, struct{}{})
		}
		o.index = key
	}
}

func (o *orderedCollection[T]) get(key int) (T, bool) {
	o.RLock()
	defer o.RUnlock()
	v, exists := o.m[key]
	return v, exists
}
