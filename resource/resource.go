package resource

import (
	"fmt"
	"time"

	"github.com/nextbillion-ai/goreman-util/asset"
	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/nextbillion-ai/gsg/lib/lock"
)

type Resource struct {
	cluster   string
	namespace string
	name      string
	spec      *global.Spec
	asset     *asset.Asset
	url       string
}

func New(rc global.ResourceContext, namespace, name string, spec *global.Spec) (*Resource, error) {
	if rc == nil {
		return nil, fmt.Errorf("empty resource context")
	}
	if namespace == "" || name == "" {
		return nil, fmt.Errorf("invalid namespace/name: %s/%s", namespace, name)
	}
	if spec == nil {
		return nil, fmt.Errorf("empty spec")
	}
	var err error
	var ass *asset.Asset
	if ass, err = asset.New(rc, spec.Asset.Typ, spec.Asset.Release); err != nil {
		return nil, err
	}
	return &Resource{
		cluster:   rc.Cluster(),
		namespace: namespace,
		name:      name,
		spec:      spec,
		asset:     ass,
		url:       fmt.Sprintf("%s/resources/%s/%s/%s.yaml", rc.BasePath(), rc.Cluster(), namespace, name),
	}, nil
}

func (r *Resource) getLockObject() (*lock.Distributed, error) {
	var err error
	var l *lock.Distributed
	if l, err = lock.NewWithUrl(r.url + ".lock"); err != nil {
		return nil, err
	}
	return l, nil
}

func (r *Resource) Rollout(rc global.ResourceContext) error {
	var err error
	var l *lock.Distributed
	if l, err = r.getLockObject(); err != nil {
		return err
	}
	defer func() { _ = l.Unlock() }()
	if err = l.Lock(rc.Context(), time.Minute*30); err != nil {
		return err
	}

	panic("implement me")
}

func (r *Resource) Uninstall() error {
	panic("implement me")
}
