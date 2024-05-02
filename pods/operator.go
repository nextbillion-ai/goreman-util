package pods

import (
	"regexp"

	"github.com/zhchang/goquiver/k8s"
)

type Operator interface {
	SpinUp(name string)
	TearDown(name string, soft bool)
	Watch(pattern *regexp.Regexp, onAvailable func(*k8s.Pod), onUnavailable func(*k8s.Pod))
}
