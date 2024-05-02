package pods

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/safe"
)

func TestGroupHP(t *testing.T) {
	op := newTO()
	var err error
	var g *Group
	if g, err = NewGroup(context.Background(), "whocares", op); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	results := safe.NewMap[int, struct{}]()

	fs := []Job{
		func(pod *k8s.Pod) error {
			defer wg.Done()
			results.Set(1, struct{}{})
			return nil
		},
		func(pod *k8s.Pod) error {
			defer wg.Done()
			results.Set(2, struct{}{})
			return nil
		},
	}
	g.Schedule(fs...)
	wg.Wait()
	count := 0
	for _, k := range results.Keys() {
		count += k
	}
	assert.Equal(t, 3, count)
}

func TestGroupIdle(t *testing.T) {
	op := newTO()
	var err error
	var g *Group
	if g, err = NewGroup(context.Background(), "whocares", op, WithIdleTimeout(10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)

	fs := []Job{
		func(pod *k8s.Pod) error {
			defer wg.Done()
			return nil
		},
		func(pod *k8s.Pod) error {
			defer wg.Done()
			return nil
		},
	}
	g.Schedule(fs...)
	wg.Wait()
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, 0, g.runners.index)
}
