package runner

import (
	"context"
	"fmt"
	"regexp"

	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/nextbillion-ai/goreman-util/resource"
	"github.com/sirupsen/logrus"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/safe"
)

type podRunner struct {
	pod      *k8s.Pod
	incoming <-chan *runJobWrapper
	requeue  chan *runJobWrapper
	cancel   func()
}

func (p *podRunner) start(ctx context.Context) {
outter:
	for {
		select {
		case <-ctx.Done():
			break outter
		default:
			select {
			case <-ctx.Done():
				break outter
			case rjw, ok := <-p.incoming:
				if !ok {
					//channel closed
					break outter
				}
				if err := rjw.run(p.pod); err != nil {
					if rjw.retryCount < rjw.retryLimit {
						rjw.retryCount++
						p.requeue <- rjw
					}
				}

			}
		}
	}
}

type runJobWrapper struct {
	run        RunJob
	retryCount uint
	retryLimit uint
}

type PodGroup struct {
	rc         global.ResourceContext
	name       string
	podPattern *regexp.Regexp
	ccf        CCF
	retry      uint
	jobs       *safe.UnlimitedChannel[*runJobWrapper]
	podRunners *safe.Map[string, *podRunner]
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

func WithCCF(ccf CCF) NewOption {
	return func(opts *PodGroup) {
		opts.ccf = ccf
	}
}

func WithRetry(retry uint) NewOption {
	return func(opts *PodGroup) {
		opts.retry = retry
	}
}

// NewPodGroup creates a new PodGroup with the specified resource context, name, and options.
// The resource context (rc) is used to interact with the Kubernetes API.
// The name is used to match the pods in the PodGroup.
// The options parameter allows for additional configuration of the PodGroup.
// Returns a pointer to the created PodGroup.
func NewPodGroup(rc global.ResourceContext, name string, options ...NewOption) *PodGroup {

	pg := &PodGroup{
		rc:         rc,
		name:       name,
		podPattern: regexp.MustCompile(fmt.Sprintf(`%s-.*`, name)),
		jobs:       safe.NewUnlimitedChannel[*runJobWrapper](),
		podRunners: safe.NewMap[string, *podRunner](),
	}
	for _, option := range options {
		option(pg)
	}
	if pg.ccf == nil {
		pg.ccf = func(*k8s.Pod) uint { return 1 }
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
		var ctx context.Context
		ctx, pr.cancel = context.WithCancel(context.Background())
		for range p.ccf(pod) {
			pr.start(ctx)
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
	}
}

var k8sWatch = k8s.WatchPods

func (p *PodGroup) WatchPods() {
	var err error
	if err = k8sWatch(p.rc.Context(), p.rc.Namespace(),
		k8s.WithWatchRegex(p.podPattern),
		k8s.WithOnAvailable(p.onAvailable),
		k8s.WithOnUnavailable(p.onUnavaiable),
	); err != nil {
		logrus.Warnf("failed to WatchPods, %s/%s: %s", p.rc.Namespace(), p.podPattern, err)
	}
}

func (p *PodGroup) finalize() {
	<-p.rc.Context().Done()
	p.jobs.Finalize()
}

func (p *PodGroup) Alter(spec *global.Spec) error {
	var err error
	var res *resource.Resource
	if res, err = resource.New(p.rc, p.name, spec); err != nil {
		return err
	}
	return res.Rollout(p.rc)
}

type RunJob func(*k8s.Pod) error

func (p *PodGroup) Schedule(jobs []RunJob) {
	for _, j := range jobs {
		p.jobs.In() <- &runJobWrapper{run: j, retryCount: 0, retryLimit: p.retry}
	}
}

func (p *PodGroup) Close() error {
	return resource.Uninstall(p.rc, p.name)
}
