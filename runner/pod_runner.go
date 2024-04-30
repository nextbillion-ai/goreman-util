package runner

import (
	"context"

	"github.com/zhchang/goquiver/k8s"
)

type podRunner struct {
	pod      *k8s.Pod
	incoming <-chan *runJobWrapper
	requeue  chan *runJobWrapper
	cancel   func()
	events   chan *event
}

func (p *podRunner) run(ctx context.Context) {
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
					continue
				}
				if p.events != nil {
					p.events <- &event{typ: eventJobDone}
				}
			}
		}
	}
}
