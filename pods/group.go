package pods

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"regexp"

	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/safe"
)

type jobWrapper struct {
	job        Job
	retryCount int
	retryLimit int
}

type Group struct {
	ctx      context.Context
	name     string
	operator Operator
	podCC    int
	retry    int
	jobs     *safe.UnlimitedChannel[*jobWrapper]
	runners  *runnerCollection
	max      int
	min      int
}

type NewOption func(*Group)

// WithPodConcurrency sets the concurrency level for the PodGroup.
// It takes a uint value representing the concurrency level and returns a NewOption function.
// The NewOption function sets the podCC field of the PodGroup struct to the provided concurrency level.
func WithPodConcurrency(cc int) NewOption {
	return func(g *Group) {
		g.podCC = cc
	}
}

// WithRetry sets the number of retries for the PodGroup.
// It returns a NewOption function that can be used to configure the PodGroup.
func WithRetry(retry int) NewOption {
	return func(g *Group) {
		g.retry = retry
	}
}

// WithMax sets the maximum value for a PodGroup option.
// It returns a NewOption function that sets the maximum value when called.
func WithMax(value int) NewOption {
	return func(g *Group) {
		g.max = value
	}
}

// WithMin sets the minimum value for the PodGroup.
func WithMin(value int) NewOption {
	return func(g *Group) {
		g.min = value
	}
}

var validNames = regexp.MustCompile(`[a-zA-Z0-9-]{1-16}`)

func getUniqueName(name string) string {
	prefix := name
	preifxNeedHash := false
	if !validNames.MatchString(prefix) {
		preifxNeedHash = true
	}
	hostname := os.Getenv("HOSTNAME")
	if hostname == "" {
		hostname = os.Getenv("HOST")
	}
	if preifxNeedHash {
		hashed := sha256.Sum256([]byte(prefix + name))
		return string(hashed[:8])
	}
	hashed := sha256.Sum256([]byte(hostname))
	return prefix + "-" + string(hashed[:8])
}

// NewPodGroup creates a new PodGroup with the specified resource context, name, and options.
// The resource context (rc) is used to interact with the Kubernetes API.
// The name is used to match the pods in the PodGroup.
// The options parameter allows for additional configuration of the PodGroup.
// Returns a pointer to the created PodGroup.
func NewGroup(ctx context.Context, name string, operator Operator, options ...NewOption) (*Group, error) {
	if operator == nil {
		return nil, fmt.Errorf("pod operator is empty")
	}

	g := &Group{
		ctx:      ctx,
		name:     getUniqueName(name),
		operator: operator,
		jobs:     safe.NewUnlimitedChannel[*jobWrapper](),
		podCC:    1,
	}
	for _, option := range options {
		option(g)
	}
	g.runners = newRunnerCollection(name, g.min, g.max, g.podCC, g.jobs.Out(), g.jobs.In(), g.operator)
	if g.podCC == 0 {
		g.podCC = 1
	}

	go operator.Watch(regexp.MustCompile(``), g.runners.onAdd, g.runners.onRemove)
	go g.finalize()
	return g, nil
}

// func (g *Group) onAvailable(pod *k8s.Pod) {
// 	var r *runner
// 	var exists bool
// 	if r, exists = g.runners.Get(pod.Name); !exists {
// 		r = &runner{
// 			pod:      pod,
// 			incoming: g.jobs.Out(),
// 			requeue:  g.jobs.In(),
// 			operator: g.operator,
// 		}
// 		var ctx context.Context
// 		ctx, r.cancel = context.WithCancel(context.Background())
// 		for range g.podCC {
// 			go r.run(ctx)
// 		}
// 	}
// 	g.runners.Set(pod.Name, r)
// }

// func (p *Group) onUnavaiable(pod *k8s.Pod) {
// 	var r *runner
// 	var exists bool
// 	if r, exists = p.runners.Get(pod.Name); exists {
// 		r.cancel()
// 		p.runners.Delete(pod.Name)
// 	}
// }

func (g *Group) finalize() {
	<-g.ctx.Done()
	g.jobs.Finalize()
}

type Job func(*k8s.Pod) error

// Schedule schedules the given jobs for execution in the pod group.
// It adds each job to the input channel of the pod group's jobs queue.
// If a strategy is set for the pod group, it also sends an event to the strategy's event receiver
// indicating the number of new jobs added.
func (g *Group) Schedule(jobs ...Job) {
	for _, j := range jobs {
		g.jobs.In() <- &jobWrapper{job: j, retryCount: 0, retryLimit: g.retry}
	}
	g.runners.schedule(len(jobs))
}
