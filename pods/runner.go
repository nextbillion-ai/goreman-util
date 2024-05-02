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
)

type runnerCollection struct {
	sync.RWMutex
	name        string
	runners     *orderedCollection[*runner]
	extractor   *regexp.Regexp
	incoming    <-chan *jobWrapper
	requeue     func(*jobWrapper)
	operator    Operator
	min         int
	max         int
	podCC       int
	jobCount    int
	jobRunning  int
	idleTimeout time.Duration
}

func newRunnerCollection(name string, min, max, podCC int, idleTimeout time.Duration, incoming <-chan *jobWrapper, requeue chan *jobWrapper, operator Operator) *runnerCollection {
	rc := &runnerCollection{
		name:        name,
		min:         min,
		max:         max,
		podCC:       podCC,
		runners:     newOrderedCollection[*runner](),
		extractor:   regexp.MustCompile(name + `-(\d+)-.*`),
		incoming:    incoming,
		operator:    operator,
		idleTimeout: idleTimeout,
	}
	rc.requeue = func(jw *jobWrapper) {
		requeue <- jw
		rc.schedule(1)
	}
	return rc
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
	var exists bool
	var r *runner
	if r, exists = rc.runners.get(index); exists {
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
		r = rc.newRunner(running)
		r.index = index
		var ctx context.Context
		ctx, r.cancel = context.WithCancel(context.Background())
		go r.start(ctx, rc.podCC, rc.idleTimeout)
		rc.runners.set(index, r)
	}
}

func (rc *runnerCollection) onRemove(pod *k8s.Pod) {
	var err error
	var index int
	if index, err = rc.extractIndex(pod.Name); err != nil {
		logrus.Warnf("runnerCollection %s onRemove triggered by pod: %s resulted in index extraction failure: %s, this should not happen", rc.name, err, pod.Name)
		return
	}
	var exists bool
	var r *runner
	if r, exists = rc.runners.get(index); exists {
		switch r.state {
		case stopping:
		case scheduled:
		case running:
			r.state = running
			r.cancel()
		}
	}
	if exists {
		rc.runners.delete(index)
	}
}

func (rc *runnerCollection) newRunner(state runnerState) *runner {
	return &runner{
		name:         rc.name,
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
		rc.runners.fill(rc.newRunner(scheduled))
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
	index        int
	soft         bool
	pod          *k8s.Pod
	state        runnerState
	incoming     <-chan *jobWrapper
	requeue      func(*jobWrapper)
	cancel       func()
	operator     Operator
	beforeJobRun func()
	afterJobRun  func()
	onJobFinish  func()
}

func (r *runner) SetIndex(index int) {
	r.index = index
	if r.state == scheduled {
		go r.operator.SpinUp(genRunnerName(r.name, r.index))
	}
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
	go r.operator.TearDown(genRunnerName(r.name, r.index), r.soft)
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
						r.requeue(rjw)
						continue
					}
				}
				r.onJobFinish()
			}
		}
	}
}
