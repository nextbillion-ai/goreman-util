package resource

import (
	"fmt"
	"time"

	"github.com/nextbillion-ai/goreman-util/asset"
	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/nextbillion-ai/goreman-util/operation"
	"github.com/nextbillion-ai/gsg/lib/lock"
	"github.com/zhchang/goquiver/raw"
)

type Resource struct {
	name  string
	spec  *global.Spec
	asset *asset.Asset
	url   string
}

func New(rc global.ResourceContext, name string, spec *global.Spec) (*Resource, error) {
	if rc == nil {
		return nil, fmt.Errorf("empty resource context")
	}
	if name == "" {
		return nil, fmt.Errorf("invalid name: %s", name)
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
		name:  name,
		spec:  spec,
		asset: ass,
		url:   fmt.Sprintf("%s/resources/%s/%s/%s.yaml", rc.BasePath(), rc.Cluster(), rc.Namespace(), name),
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

type rolloutOptions struct {
	values map[string]any
}

type RolloutOption func(*rolloutOptions)

func WithValues(values map[string]any) RolloutOption {
	return func(ros *rolloutOptions) {
		ros.values = values
	}
}

func (r *Resource) Rollout(rc global.ResourceContext, options ...RolloutOption) error {
	var err error
	var l *lock.Distributed
	if l, err = r.getLockObject(); err != nil {
		return err
	}
	defer func() { _ = l.Unlock() }()
	if err = l.Lock(rc.Context(), time.Minute*30); err != nil {
		return err
	}
	ros := &rolloutOptions{}
	for _, option := range options {
		option(ros)
	}
	var g map[string]any
	if g, err = global.GlobalSpec(rc.Cluster()); err != nil {
		return err
	}
	g = raw.Merge(g, map[string]any{
		"name":      r.name,
		"namespace": rc.Namespace(),
	})
	fmt.Printf("%+v\n", g)
	app := raw.Merge(r.spec.App, ros.values)
	values := map[string]any{"app": app, "global": g}
	//fmt.Printf("%+v\n", values)
	if err = r.asset.Validate(app); err != nil {
		return err
	}
	return operation.Rollout(rc, r.asset.ChartPath(), values)
}

func (r *Resource) Uninstall() error {
	panic("implement me")
}
