package runner

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zhchang/goquiver/k8s"
)

type scaleEvent struct {
	onAdd    func(*k8s.Pod)
	onRemove func(*k8s.Pod)
	size     uint
}

func TestStrategyHP(t *testing.T) {
	scaleCh := make(chan *scaleEvent)
	var orgK8sWatch = k8sWatch
	defer func() {
		k8sWatch = orgK8sWatch
	}()
	currentCount := uint(0)
	k8sWatch = func(_ context.Context, _ string, _ ...k8s.WatchOption) error {
		for {
			se := <-scaleCh
			if se == nil {
				return nil
			}
			for i := currentCount; i < se.size; i++ {
				se.onAdd(mockPod(t, strconv.Itoa(int(i))))
			}
			for i := se.size; i < currentCount; i++ {
				se.onRemove(mockPod(t, strconv.Itoa(int(i))))
			}
			currentCount = se.size
		}
	}
	pg := NewPodGroup(context.Background(), "whocares", "whocares")
	pg.strategy = NewFastScaling(pg.ctx, func(size uint) {
		scaleCh <- &scaleEvent{onAdd: pg.onAvailable, onRemove: pg.onUnavaiable, size: size}
	}, 0, 0, 1)
	var wg sync.WaitGroup
	count := 0
	fs := []RunJob{}
	for range 10 {
		wg.Add(1)
		fs = append(fs, func(pod *k8s.Pod) error {
			defer wg.Done()
			count++
			return nil
		})
	}
	pg.Schedule(fs...)
	wg.Wait()
	assert.Equal(t, 10, count)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint(0), currentCount)
}
