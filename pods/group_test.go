package pods

import (
	"context"
	"fmt"
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

func TestGroupReschedule(t *testing.T) {
	op := newTO()
	var err error
	var g *Group
	if g, err = NewGroup(context.Background(), "whocares", op, WithRetry(1)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	doneCh := make(chan struct{})
	job1 := 0
	job2 := 0

	fs := []Job{
		func(pod *k8s.Pod) error {
			if job1 == 0 {
				time.Sleep(10 * time.Millisecond)
				job1++
				return fmt.Errorf("whocares")
			}
			defer wg.Done()
			return nil
		},
		func(pod *k8s.Pod) error {
			if job2 == 0 {
				time.Sleep(10 * time.Millisecond)
				job2++
				return fmt.Errorf("whocares")
			}
			defer wg.Done()
			return nil
		},
	}
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	g.Schedule(fs...)
	go func() {
		time.Sleep(5 * time.Millisecond)
		keys := g.runners.runners.Keys()
		for _, key := range keys {
			if r, exists := g.runners.runners.Get(key); exists {
				if r.cancel != nil {
					r.cancel()
				}
			}
		}
	}()
	timeout := time.NewTicker(50 * time.Millisecond)
	select {
	case <-timeout.C:
		t.Fatal("timeout happened")
	case <-doneCh:
	}
}
