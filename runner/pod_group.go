// Package runner provides utilities for managing and manipulating Kubernetes runners.
// It includes functionalities for handling runner operations such as scaling, watching,
// and retrying operations.
//
// The package uses the "regexp" standard library for matching pod patterns, and
// "github.com/zhchang/goquiver/k8s" for Kubernetes operations.
//
// The main type in this package is PodGroup, which represents a group of pods in a
// Kubernetes runner. It provides methods for setting pod patterns, concurrency, retry
// attempts, and scaling strategy.
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

type NewOption func(*PodGroup)

// WithPodPattern sets the regular expression pattern for matching pods in a PodGroup.
// It takes a compiled regular expression as input and returns a NewOption function.
// The NewOption function sets the podPattern field of the PodGroup options.
func WithPodPattern(re *regexp.Regexp) NewOption {
	return func(opts *PodGroup) {
		opts.podPattern = re
	}
}

// WithPodConcurrency sets the concurrency level for the PodGroup.
// It takes a uint value representing the concurrency level and returns a NewOption function.
// The NewOption function sets the podCC field of the PodGroup struct to the provided concurrency level.
func WithPodConcurrency(cc uint) NewOption {
	return func(opts *PodGroup) {
		opts.podCC = cc
	}
}

// WithRetry sets the number of retries for the PodGroup.
// It returns a NewOption function that can be used to configure the PodGroup.
func WithRetry(retry uint) NewOption {
	return func(opts *PodGroup) {
		opts.retry = retry
	}
}

// WithStrategy sets the strategy for the PodGroup.
// The strategy determines how the PodGroup behaves when starting and stopping pods.
func WithStrategy(s Strategy) NewOption {
	return func(opts *PodGroup) {
		opts.strategy = s
	}
}

// WithScaler is a NewOption that sets the scaler function for the PodGroup.
// The scaler function is used to adjust the number of pods in the group.
func WithScaler(scaler func(uint)) NewOption {
	return func(opts *PodGroup) {
		opts.scaler = scaler
	}
}

// WithMax sets the maximum value for a PodGroup option.
// It returns a NewOption function that sets the maximum value when called.
func WithMax(value uint) NewOption {
	return func(opts *PodGroup) {
		opts.max = value
	}
}

// WithMin sets the minimum value for the PodGroup.
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

// WatchPods watches for pods in the specified namespace that match the pod pattern.
// It uses the k8sWatch function to perform the watch operation.
// If any error occurs during the watch, a warning message is logged.
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

// Schedule schedules the given jobs for execution in the pod group.
// It adds each job to the input channel of the pod group's jobs queue.
// If a strategy is set for the pod group, it also sends an event to the strategy's event receiver
// indicating the number of new jobs added.
func (p *PodGroup) Schedule(jobs ...RunJob) {
	for _, j := range jobs {
		p.jobs.In() <- &runJobWrapper{run: j, retryCount: 0, retryLimit: p.retry}
	}
	if p.strategy != nil {
		p.strategy.EventReceiver() <- &event{typ: eventNewJobs, payload: uint(len(jobs))}
	}
}
