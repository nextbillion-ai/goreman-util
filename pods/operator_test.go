package pods

import (
	"regexp"

	"github.com/zhchang/goquiver/k8s"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type testOperator struct {
	adds    chan *k8s.Pod
	removes chan *k8s.Pod
}

func newTO() Operator {
	return &testOperator{
		adds:    make(chan *k8s.Pod, 1000),
		removes: make(chan *k8s.Pod, 1000),
	}
}

func (to *testOperator) SpinUp(name string) {
	to.adds <- &k8s.Pod{ObjectMeta: v1.ObjectMeta{Name: name + "-0"}}
}

func (to *testOperator) TearDown(name string, _ bool) {
	to.removes <- &k8s.Pod{ObjectMeta: v1.ObjectMeta{Name: name + "-0"}}
}

func (to *testOperator) Watch(pattern *regexp.Regexp, onAdd func(*k8s.Pod), onRemove func(*k8s.Pod)) {
	for {
		select {
		case add := <-to.adds:
			if pattern.MatchString(add.Name) {
				onAdd(add)
			}
		case remove := <-to.removes:
			if pattern.MatchString(remove.Name) {
				onRemove(remove)
			}
		}
	}
}
