package global

import (
	"context"
	"io"
	"os"
	"time"

	yaml "gopkg.in/yaml.v3"
)

type Spec struct {
	Asset struct {
		Typ     string `yaml:"type"`
		Release string `yaml:"release"`
	} `yaml:"asset"`
	App map[string]any `yaml:"app"`
}

func SpecFromYaml(input any) (*Spec, error) {
	var err error
	var data []byte
	switch v := input.(type) {
	case []byte:
		data = v
	case string:
		if data, err = os.ReadFile(v); err != nil {
			return nil, err
		}
	case io.ReadCloser:
		defer v.Close()
		if data, err = io.ReadAll(v); err != nil {
			return nil, err
		}
	}

	var spec Spec
	if err = yaml.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

type AssetContext interface {
	WorkPath() string
	BasePath() string
}

type ResourceContext interface {
	AssetContext
	Cluster() string
	Context() context.Context
	Namespace() string
	Timeout() time.Duration
}

type rcImpl struct {
	cluster   string
	namespace string
	workPath  string
	basePath  string
	ctx       context.Context
	timeout   time.Duration
}

// BasePath implements ResourceContext.
func (r *rcImpl) BasePath() string {
	return r.basePath
}

// Cluster implements ResourceContext.
func (r *rcImpl) Cluster() string {
	return r.cluster
}

// Context implements ResourceContext.
func (r *rcImpl) Context() context.Context {
	return r.ctx
}

// Namespace implements ResourceContext.
func (r *rcImpl) Namespace() string {
	return r.namespace
}

// Timeout implements ResourceContext.
func (r *rcImpl) Timeout() time.Duration {
	return r.timeout
}

// WorkPath implements ResourceContext.
func (r *rcImpl) WorkPath() string {
	return r.workPath
}

type ContextOption func(*rcImpl)

func WithNamespace(namespace string) ContextOption {
	return func(r *rcImpl) { r.namespace = namespace }
}

func WithCluster(cluster string) ContextOption {
	return func(r *rcImpl) { r.cluster = cluster }
}

func WithBasePath(basePath string) ContextOption {
	return func(r *rcImpl) { r.basePath = basePath }
}
func WithWorkPath(workPath string) ContextOption {
	return func(r *rcImpl) { r.workPath = workPath }
}
func WithTimeout(timeout time.Duration) ContextOption {
	return func(r *rcImpl) { r.timeout = timeout }
}

func NewContext(ctx context.Context, options ...ContextOption) ResourceContext {
	r := &rcImpl{ctx: ctx}
	for _, option := range options {
		option(r)
	}
	return r
}
