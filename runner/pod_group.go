package runner

import (
	"context"
	"fmt"
	"regexp"

	"github.com/sirupsen/logrus"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/safe"
)

type runJobWrapper struct {
	run        RunJob
	retryCount uint
	retryLimit uint
}

type PodGroup struct {
	ctx        context.Context
	name       string
	namespace  string
	podPattern *regexp.Regexp
	podCC      uint
	retry      uint
	jobs       *safe.UnlimitedChannel[*runJobWrapper]
	podRunners *safe.Map[string, *podRunner]
	strategy   Strategy
	scaler     func(uint)
	max        uint
	min        uint
}

// CCF is a function to calculate concurrency a pod could reach
// if CCF is not supplied when creating a PodGroup, it will default to a function that always return 1
type CCF func(*k8s.Pod) uint

type NewOption func(*PodGroup)

func WithPodPattern(re *regexp.Regexp) NewOption {
	return func(opts *PodGroup) {
		opts.podPattern = re
	}
}

func WithPodConcurrency(cc uint) NewOption {
	return func(opts *PodGroup) {
		opts.podCC = cc
	}
}

func WithRetry(retry uint) NewOption {
	return func(opts *PodGroup) {
		opts.retry = retry
	}
}

func WithStrategy(s Strategy) NewOption {
	return func(opts *PodGroup) {
		opts.strategy = s
	}
}

func WithScaler(scaler func(uint)) NewOption {
	return func(opts *PodGroup) {
		opts.scaler = scaler
	}
}

func WithMax(value uint) NewOption {
	return func(opts *PodGroup) {
		opts.max = value
	}
}
func WithMin(value uint) NewOption {
	return func(opts *PodGroup) {
		opts.min = value
	}
}

// NewPodGroup creates a new PodGroup with the specified resource context, name, and options.
// The resource context (rc) is used to interact with the Kubernetes API.
// The name is used to match the pods in the PodGroup.
// The options parameter allows for additional configuration of the PodGroup.
// Returns a pointer to the created PodGroup.
func NewPodGroup(ctx context.Context, name, namespace string, options ...NewOption) *PodGroup {

	pg := &PodGroup{
		ctx:        ctx,
		name:       name,
		namespace:  namespace,
		podPattern: regexp.MustCompile(fmt.Sprintf(`%s-.*`, name)),
		jobs:       safe.NewUnlimitedChannel[*runJobWrapper](),
		podRunners: safe.NewMap[string, *podRunner](),
		podCC:      1,
	}
	for _, option := range options {
		option(pg)
	}
	if pg.podCC == 0 {
		pg.podCC = 1
	}
	if pg.strategy == nil {
		if pg.scaler != nil {
			pg.strategy = NewFastScaling(ctx, pg.scaler, pg.max, pg.min, pg.podCC)
		}
	}

	go pg.WatchPods()
	go pg.finalize()
	return pg
}

func (p *PodGroup) onAvailable(pod *k8s.Pod) {
	var pr *podRunner
	var exists bool
	if pr, exists = p.podRunners.Get(pod.Name); !exists {
		pr = &podRunner{
			pod:      pod,
			incoming: p.jobs.Out(),
			requeue:  p.jobs.In(),
		}
		if p.strategy != nil {
			pr.events = p.strategy.EventReceiver()
		}
		var ctx context.Context
		ctx, pr.cancel = context.WithCancel(context.Background())
		for range p.podCC {
			go pr.run(ctx)
		}
		if p.strategy != nil {
			p.strategy.EventReceiver() <- &event{typ: eventPodAdded}
		}
	}
	p.podRunners.Set(pod.Name, pr)
}

func (p *PodGroup) onUnavaiable(pod *k8s.Pod) {
	var pr *podRunner
	var exists bool
	if pr, exists = p.podRunners.Get(pod.Name); exists {
		pr.cancel()
		p.podRunners.Delete(pod.Name)
		if p.strategy != nil {
			p.strategy.EventReceiver() <- &event{typ: eventPodAdded}
		}
	}
}

var k8sWatch = k8s.WatchPods

func (p *PodGroup) WatchPods() {
	var err error
	if err = k8sWatch(p.ctx, p.namespace,
		k8s.WithWatchRegex(p.podPattern),
		k8s.WithOnAvailable(p.onAvailable),
		k8s.WithOnUnavailable(p.onUnavaiable),
	); err != nil {
		logrus.Warnf("failed to WatchPods, %s/%s: %s", p.namespace, p.podPattern, err)
	}
}

func (p *PodGroup) finalize() {
	<-p.ctx.Done()
	p.jobs.Finalize()
}

type RunJob func(*k8s.Pod) error

func (p *PodGroup) Schedule(jobs ...RunJob) {
	for _, j := range jobs {
		p.jobs.In() <- &runJobWrapper{run: j, retryCount: 0, retryLimit: p.retry}
	}
	if p.strategy != nil {
		p.strategy.EventReceiver() <- &event{typ: eventNewJobs, payload: uint(len(jobs))}
	}
}
