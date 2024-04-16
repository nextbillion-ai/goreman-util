package global

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v3"
)

// Spec represents a specification that includes information about an asset and an application.
// It is used to parse specifications from YAML files or other types of input data.
//
// Fields:
// Asset: A struct that contains the type and release version of the asset.
// App: A map that contains application-specific data.
//
// The Asset field is tagged with `yaml:"asset"` to specify the YAML name of the field.
// The App field is tagged with `yaml:"app"` to specify the YAML name of the field.
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

type Plugin struct {
	Name string
	Url  string
	Keys []string
}

type AssetContext interface {
	WorkPath() string
}

// ResourceContext is an interface that extends the AssetContext interface and
// provides additional methods for accessing the cluster, context, namespace,
// timeout, and logger associated with a resource.
//
// Methods:
// Cluster: Returns the cluster associated with the resource.
// Context: Returns the context associated with the resource.
// Namespace: Returns the namespace associated with the resource.
// Timeout: Returns the timeout duration for operations on the resource.
// Logger: Returns the logger associated with the resource.
type ResourceContext interface {
	AssetContext
	Context() context.Context
	Namespace() string
	Timeout() time.Duration
	Logger() *logrus.Logger
	Plugins() []*Plugin
}

type rcImpl struct {
	namespace string
	workPath  string
	ctx       context.Context
	timeout   time.Duration
	logger    *logrus.Logger
	plugins   []*Plugin
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

func (r *rcImpl) Logger() *logrus.Logger {
	return r.logger
}

func (r *rcImpl) Plugins() []*Plugin {
	return r.plugins
}

type ContextOption func(*rcImpl)

func WithNamespace(namespace string) ContextOption {
	return func(r *rcImpl) { r.namespace = namespace }
}

func WithWorkPath(workPath string) ContextOption {
	return func(r *rcImpl) { r.workPath = workPath }
}
func WithTimeout(timeout time.Duration) ContextOption {
	return func(r *rcImpl) { r.timeout = timeout }
}

func WithLogLevel(level logrus.Level) ContextOption {
	return func(r *rcImpl) { r.logger.SetLevel(level) }
}

func WithPlugins(plugins []*Plugin) ContextOption {
	return func(r *rcImpl) { r.plugins = plugins }
}

func NewContext(ctx context.Context, options ...ContextOption) ResourceContext {
	r := &rcImpl{
		ctx:    ctx,
		logger: logrus.New(),
	}
	for _, option := range options {
		option(r)
	}
	return r
}
