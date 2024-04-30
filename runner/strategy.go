package runner

import (
	"context"
	"runtime/debug"

	"github.com/sirupsen/logrus"
	"github.com/zhchang/goquiver/safe"
)

type eventType int

const (
	eventNewJobs eventType = iota
	eventJobDone
	eventPodAdded
	eventPodRemoved
)

type event struct {
	typ     eventType
	payload uint
}

type Strategy interface {
	EventReceiver() chan *event
}

type FastScaling struct {
	ctx      context.Context
	jobCount uint
	podCount uint
	podCC    uint
	max      uint
	min      uint
	scaler   func(uint)
	events   *safe.UnlimitedChannel[*event]
}

// NewFastScaling creates a new instance of the FastScaling strategy.
// It takes a context, a scaler function, maximum and minimum values for scaling,
// and the number of pods to be created or destroyed at each scaling event.
// It returns a pointer to the created FastScaling instance.
func NewFastScaling(ctx context.Context, scaler func(uint), max, min uint, podCC uint) *FastScaling {
	s := &FastScaling{
		ctx:    ctx,
		max:    max,
		min:    min,
		scaler: scaler,
		podCC:  podCC,
		events: safe.NewUnlimitedChannel[*event](),
	}
	go s.process()
	return s
}

func (f *FastScaling) doScale() {
	podsNeeded := f.jobCount / f.podCC
	if f.jobCount%f.podCC > 0 {
		podsNeeded++
	}
	if f.max > 0 && podsNeeded > f.max {
		podsNeeded = f.max
	}
	if f.min > 0 && podsNeeded < f.min {
		podsNeeded = f.min
	}
	if podsNeeded != f.podCount {
		f.podCount = podsNeeded
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logrus.Errorf("[Scaler Panic]: %s", string(debug.Stack()))
				}
			}()
			f.scaler(f.podCount)
		}()
	}
}

func (f *FastScaling) process() {
outer:
	for {
		select {
		case <-f.ctx.Done():
			break outer
		default:
			select {
			case <-f.ctx.Done():
				break outer
			case evt := <-f.events.Out():
				switch evt.typ {
				case eventNewJobs:
					f.jobCount += evt.payload
					f.doScale()
				case eventJobDone:
					f.jobCount--
					f.doScale()
				case eventPodRemoved:
					f.podCount--
					f.doScale()
				}
			}
		}
	}
	f.events.Finalize()

}

// EventReceiver returns a channel that can be used to receive events.
func (f *FastScaling) EventReceiver() chan *event {
	return f.events.In()
}
