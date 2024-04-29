package runner

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/stretchr/testify/assert"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/safe"
)

var podTemplate = `
apiVersion: v1
kind: Pod
metadata:
  name: "%s"
`

func mockPod(t *testing.T, name string) *k8s.Pod {
	var res k8s.Resource
	var err error
	if res, err = k8s.DecodeYAML(fmt.Sprintf(podTemplate, name)); err != nil {
		t.Fatal(err)
	}
	var pod *k8s.Pod
	if pod, err = k8s.Parse[*k8s.Pod](res); err != nil {
		t.Fatal(err)
	}
	return pod
}

func TestPodGroupHP(t *testing.T) {
	var orgK8sWatch = k8sWatch
	defer func() {
		k8sWatch = orgK8sWatch
	}()
	k8sWatch = func(_ context.Context, _ string, _ ...k8s.WatchOption) error {
		return nil
	}
	expectedPodName := "testpod1"
	rc := global.NewContext(context.Background())
	pg := NewPodGroup(rc, "whocares")
	pod := mockPod(t, expectedPodName)
	pg.onAvailable(pod)
	actualPodName := ""
	ch := make(chan struct{})
	pg.Schedule(func(p *k8s.Pod) error {
		actualPodName = p.Name
		close(ch)
		return nil
	})
	<-ch
	assert.Equal(t, expectedPodName, actualPodName)
}

func TestPodGroupMoreJobLessPods(t *testing.T) {
	var orgK8sWatch = k8sWatch
	defer func() {
		k8sWatch = orgK8sWatch
	}()
	k8sWatch = func(_ context.Context, _ string, _ ...k8s.WatchOption) error {
		return nil
	}
	expectedPod1 := "testpod1"
	expectedPod2 := "testpod2"
	pod1 := mockPod(t, expectedPod1)
	pod2 := mockPod(t, expectedPod2)
	rc := global.NewContext(context.Background())
	pg := NewPodGroup(rc, "whocares")
	pg.onAvailable(pod1)
	pg.onAvailable(pod2)
	var wg sync.WaitGroup
	count := 0
	podMap := safe.NewMap[string, int]()
	podMap.Set(expectedPod1, 0)
	podMap.Set(expectedPod2, 0)
	fs := []RunJob{}
	for range 10 {
		wg.Add(1)
		fs = append(fs, func(pod *k8s.Pod) error {
			defer wg.Done()
			count++
			p, _ := podMap.Get(pod.Name)
			podMap.Set(pod.Name, p+1)
			return nil
		})
	}
	pg.Schedule(fs...)
	wg.Wait()
	assert.Equal(t, 10, count)
	p1, _ := podMap.Get(expectedPod1)
	p2, _ := podMap.Get(expectedPod2)
	assert.Equal(t, 10, p1+p2)
}

func TestPodGroupRetry(t *testing.T) {
	var orgK8sWatch = k8sWatch
	defer func() {
		k8sWatch = orgK8sWatch
	}()
	k8sWatch = func(_ context.Context, _ string, _ ...k8s.WatchOption) error {
		return nil
	}
	expectedPod1 := "testpod1"
	expectedPod2 := "testpod2"
	pod1 := mockPod(t, expectedPod1)
	pod2 := mockPod(t, expectedPod2)
	rc := global.NewContext(context.Background())
	pg := NewPodGroup(rc, "whocares", WithRetry(3))
	pg.onAvailable(pod1)
	pg.onAvailable(pod2)
	var wg sync.WaitGroup
	count := 0
	podMap := safe.NewMap[string, int]()
	podMap.Set(expectedPod1, 0)
	podMap.Set(expectedPod2, 0)
	retryMap := safe.NewMap[int, int]()
	fs := []RunJob{}
	for i := range 10 {
		retryMap.Set(i, 0)
		wg.Add(1)
		fs = append(fs, func(pod *k8s.Pod) error {
			count++
			p, _ := podMap.Get(pod.Name)
			podMap.Set(pod.Name, p+1)
			r, _ := retryMap.Get(i)
			if r < 3 {
				retryMap.Set(i, r+1)
				return fmt.Errorf("need retry")
			}
			wg.Done()
			return nil
		})
	}
	pg.Schedule(fs...)
	wg.Wait()
	assert.Equal(t, 40, count)
	p1, _ := podMap.Get(expectedPod1)
	p2, _ := podMap.Get(expectedPod2)
	assert.Equal(t, 40, p1+p2)
}
