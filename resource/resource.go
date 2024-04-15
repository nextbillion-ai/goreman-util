// Package resource provides utilities for managing and manipulating Kubernetes resources.
// It includes functionalities for handling resource operations such as installation,
// uninstallation, and validation.
//
// The package uses the "github.com/nextbillion-ai/goreman-util/global" library for global
// configurations and specifications, "github.com/zhchang/goquiver/k8s" for Kubernetes
// operations, and "github.com/zhchang/goquiver/raw" for raw data operations.
//
// The main type in this package is Resource, which represents a Kubernetes resource.
// It provides methods for installing, uninstalling, and validating the resource.
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

// New creates a new Resource instance with the given resource context, name, and spec.
// It returns a pointer to the created Resource and an error, if any.
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

type resourceOptions struct {
	values map[string]any
	wait   time.Duration
}

type ResourceOption func(*resourceOptions)

// WithValues sets the values for a resource option.
// It takes a map of string to any as input and returns a ResourceOption function.
// The returned function sets the values of the resource options.
func WithValues(values map[string]any) ResourceOption {
	return func(ros *resourceOptions) {
		ros.values = values
	}
}

// WithWait sets the wait duration for a resource option.
func WithWait(wait time.Duration) ResourceOption {
	return func(ros *resourceOptions) {
		ros.wait = wait
	}
}

// Rollout performs a resource rollout operation.
// It acquires a lock, merges global and app-specific options, validates the asset,
// and then triggers the rollout operation using the provided resource context and options.
// The function returns an error if any of the operations fail.
func (r *Resource) Rollout(rc global.ResourceContext, options ...ResourceOption) error {
	var err error
	var l *lock.Distributed
	if l, err = r.getLockObject(); err != nil {
		return err
	}
	defer func() { _ = l.Unlock() }()
	if err = l.Lock(rc.Context(), time.Minute*30); err != nil {
		return err
	}
	ros := &resourceOptions{}
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
	app := raw.Merge(r.spec.App, ros.values)
	values := map[string]any{"app": app, "global": g}
	//fmt.Printf("%+v\n", values)
	if err = r.asset.Validate(app); err != nil {
		return err
	}
	oos := []operation.OperationOption{}
	if ros.wait > 0 {
		oos = append(oos, operation.WithWait(ros.wait))
	}
	return operation.Rollout(rc, r.asset.ChartPath(), values, oos...)
}

// Uninstall removes the resource from the cluster.
// It takes a global.ResourceContext and optional ResourceOption(s) as parameters.
// The ResourceOptions can be used to customize the uninstallation process.
// If the wait duration is specified in the ResourceOptions, it will wait for the resource to be removed before returning.
// It returns an error if the uninstallation fails.
func (r *Resource) Uninstall(rc global.ResourceContext, options ...ResourceOption) error {
	ros := &resourceOptions{}
	for _, option := range options {
		option(ros)
	}
	oos := []operation.OperationOption{}
	if ros.wait > 0 {
		oos = append(oos, operation.WithWait(ros.wait))
	}
	return operation.Remove(rc, r.name, rc.Namespace(), oos...)
}
