package pods

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/safe"
)

type runnerCollection struct {
	sync.RWMutex
	name        string
	runners     *safe.Map[string, *runner]
	holes       map[int]struct{}
	index       int
	extractor   *regexp.Regexp
	incoming    <-chan *jobWrapper
	requeue     chan *jobWrapper
	operator    Operator
	min         int
	max         int
	podCC       int
	jobCount    int
	jobRunning  int
	idleTimeout time.Duration
}

func newRunnerCollection(name string, min, max, podCC int, idleTimeout time.Duration, incoming <-chan *jobWrapper, requeue chan *jobWrapper, operator Operator) *runnerCollection {
	return &runnerCollection{
		name:        name,
		min:         min,
		max:         max,
		podCC:       podCC,
		runners:     safe.NewMap[string, *runner](),
		extractor:   regexp.MustCompile(name + `-(\d+)-.*`),
		incoming:    incoming,
		requeue:     requeue,
		operator:    operator,
		holes:       map[int]struct{}{},
		idleTimeout: idleTimeout,
	}
}

func (rc *runnerCollection) onJobFinish() {
	rc.Lock()
	defer rc.Unlock()
	rc.jobCount--
}

func (rc *runnerCollection) beforeJobRun() {
	rc.Lock()
	defer rc.Unlock()
	rc.jobRunning++
}

func (rc *runnerCollection) afterJobRun() {
	rc.Lock()
	defer rc.Unlock()
	rc.jobRunning--
}

func (rc *runnerCollection) extractIndex(name string) (int, error) {
	matches := rc.extractor.FindStringSubmatch(name)
	if len(matches) > 1 {
		return strconv.Atoi(matches[1])
	}
	return 0, fmt.Errorf("invalid name")
}

func genRunnerName(name string, index int) string {
	return name + "-" + strconv.Itoa(index)
}

func (rc *runnerCollection) onAdd(pod *k8s.Pod) {
	var err error
	var index int
	if index, err = rc.extractIndex(pod.Name); err != nil {
		logrus.Warnf("runnerCollection %s onAdd triggered by pod: %s resulted in index extraction failure: %s, this should not happen", rc.name, err, pod.Name)
		return
	}
	runnerName := genRunnerName(rc.name, index)
	var exists bool
	var r *runner
	if r, exists = rc.runners.Get(runnerName); exists {
		switch r.state {
		case stopping:
			r = nil
		case scheduled:
			r.state = running
			var ctx context.Context
			ctx, r.cancel = context.WithCancel(context.Background())
			go r.start(ctx, rc.podCC, rc.idleTimeout)
		case running:
			logrus.Warnf("runnerCollection %s onAdd triggered by pod: %s when runner is already running, this should not happen", rc.name, pod.Name)
			return
		}
	}
	if r == nil {
		r = rc.newRunner(runnerName, running)
		var ctx context.Context
		ctx, r.cancel = context.WithCancel(context.Background())
		go r.start(ctx, rc.podCC, rc.idleTimeout)
		func() {
			rc.Lock()
			defer rc.Unlock()
			rc.runners.Set(runnerName, r)
			if index > rc.index {
				for i := rc.index; i < index; i++ {
					rc.holes[i] = struct{}{}
				}
				rc.index = index
			}
		}()
	}
}

func (rc *runnerCollection) onRemove(pod *k8s.Pod) {
	var err error
	var index int
	if index, err = rc.extractIndex(pod.Name); err != nil {
		logrus.Warnf("runnerCollection %s onRemove triggered by pod: %s resulted in index extraction failure: %s, this should not happen", rc.name, err, pod.Name)
		return
	}
	runnerName := genRunnerName(rc.name, index)
	var exists bool
	var r *runner
	if r, exists = rc.runners.Get(runnerName); exists {
		switch r.state {
		case stopping:
			rc.runners.Delete(runnerName)
		case scheduled:
			rc.runners.Delete(runnerName)
		case running:
			r.state = running
			r.cancel()
			rc.runners.Delete(runnerName)
		}
	}
	func() {
		//shrink index and holes where possible
		rc.Lock()
		defer rc.Unlock()
		if index == rc.index {
			rc.index--
		} else if index < rc.index {
			rc.holes[index] = struct{}{}
		}
		for {
			if _, exists := rc.holes[rc.index]; exists {
				delete(rc.holes, rc.index)
				rc.index--
				continue
			}
			break
		}
	}()
}

func (rc *runnerCollection) newRunner(name string, state runnerState) *runner {
	if state == scheduled {
		rc.operator.SpinUp(name)
	}
	return &runner{
		name:         name,
		state:        state,
		incoming:     rc.incoming,
		requeue:      rc.requeue,
		operator:     rc.operator,
		beforeJobRun: rc.beforeJobRun,
		afterJobRun:  rc.afterJobRun,
		onJobFinish:  rc.onJobFinish,
	}
}

func (rc *runnerCollection) schedule(count int) {
	rc.Lock()
	defer rc.Unlock()
	rc.jobCount += count
	needRun := rc.jobCount - rc.jobRunning
	runnersNeeded := needRun / rc.podCC
	if needRun%rc.podCC > 0 {
		runnersNeeded++
	}
	for range runnersNeeded {
		if len(rc.holes) > 0 {
			for k := range rc.holes {
				runnerName := genRunnerName(rc.name, k)
				rc.runners.Set(runnerName, rc.newRunner(runnerName, scheduled))
				delete(rc.holes, k)
				break
			}
			continue
		}
		rc.index++
		runnerName := genRunnerName(rc.name, rc.index)
		rc.runners.Set(runnerName, rc.newRunner(runnerName, scheduled))
	}
}

type runnerState int

const (
	scheduled runnerState = iota
	running
	stopping
)

type runner struct {
	name         string
	soft         bool
	pod          *k8s.Pod
	state        runnerState
	incoming     <-chan *jobWrapper
	requeue      chan *jobWrapper
	cancel       func()
	operator     Operator
	beforeJobRun func()
	afterJobRun  func()
	onJobFinish  func()
}

func (r *runner) start(ctx context.Context, count int, idleTimeout time.Duration) {
	r.state = running
	busy := make(chan struct{})
	for range count {
		go r.run(ctx, busy)
	}
outter:
	for {
		idleTicker := time.NewTicker(idleTimeout)
		select {
		case <-ctx.Done():
			break outter
		case <-idleTicker.C:
			r.cancel()
			break outter
		case <-busy:
		}
		idleTicker.Stop()
	}
	r.state = stopping
	r.operator.TearDown(r.name, r.soft)
}

func (r *runner) run(ctx context.Context, busy chan struct{}) {
outter:
	for {
		select {
		case <-ctx.Done():
			break outter
		default:
			select {
			case <-ctx.Done():
				break outter
			case rjw, ok := <-r.incoming:
				if !ok {
					//channel closed
					break outter
				}
				busy <- struct{}{}
				if err := func() error {
					r.beforeJobRun()
					defer r.afterJobRun()
					return rjw.job(r.pod)
				}(); err != nil {
					if rjw.retryCount < rjw.retryLimit {
						rjw.retryCount++
						r.requeue <- rjw
						continue
					}
				}
				r.onJobFinish()
			}
		}
	}
}
