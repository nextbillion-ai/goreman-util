package global

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/nextbillion-ai/gsg/lib/object"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/raw"
	"gopkg.in/yaml.v3"
)

var globalOptionsOnce sync.Once

type Options struct {
	Cluster  string
	Basepath string
	Values   raw.Map
}

var _globalOptions *Options

var readGCSYaml = func(url string) (values raw.Map, err error) {
	var o *object.Object
	if o, err = object.New(url); err != nil {
		return
	}
	var buf bytes.Buffer
	if err = o.Read(&buf); err != nil {
		return
	}
	values = raw.Map{}
	if err = yaml.Unmarshal(buf.Bytes(), values); err != nil {
		return
	}
	err = nil
	return
}

var GlobalSpec = func(rc ResourceContext, name string, appValue raw.Map) (spec raw.Map, err error) {
	if _globalOptions == nil {
		err = fmt.Errorf("not properly inited")
		return
	}
	spec = _globalOptions.Values

	for _, plugin := range rc.Plugins() {
		if plugin.Name == "" || plugin.Url == "" || len(plugin.Keys) == 0 {
			rc.Logger().Warnf("plugin %s not loaded because its name/url/keys are empty", plugin.Name)
			continue
		}
		plugin.Url = strings.ReplaceAll(plugin.Url, `{cluster}`, _globalOptions.Cluster)
		plugin.Url = strings.ReplaceAll(plugin.Url, `{namespace}`, rc.Namespace())
		plugin.Url = strings.ReplaceAll(plugin.Url, `{name}`, name)
		for _, item := range []string{"area", "mode", "context"} {
			if strings.Contains(plugin.Url, item) {
				var value string
				if value, err = raw.Get[string](appValue, item); err != nil {
					rc.Logger().Warnf("plugin failed to load: %s not found in app values", item)
					return
				}
				plugin.Url = strings.ReplaceAll(plugin.Url, "{"+item+"}", value)
			}

		}
		var values raw.Map
		if values, err = readGCSYaml(plugin.Url); err != nil {
			return
		}
		object := raw.Map{}
		for _, key := range plugin.Keys {
			object[key] = values[key]
		}
		spec[plugin.Name] = object
	}
	return
}

var MustHaveOptions = func() *Options {
	if _globalOptions == nil {
		panic("not initialized")
	}
	return _globalOptions
}

func InitFromConfigMap(name, namespace string) (err error) {
	globalOptionsOnce.Do(func() {
		_globalOptions = &Options{}
		var cfgMap *k8s.ConfigMap
		if cfgMap, err = k8s.Get[*k8s.ConfigMap](context.Background(), name, namespace); err != nil {
			return
		}
		_globalOptions.Cluster = cfgMap.Data["CLUSTER"]
		_globalOptions.Basepath = cfgMap.Data["OP_BASEPATH"]
		if _globalOptions.Cluster == "" {
			err = fmt.Errorf("failed to read config from cluster")
			return
		}
		if _globalOptions.Basepath == "" {
			_globalOptions.Basepath = "gs://fm-op-" + _globalOptions.Cluster
		}
		var _that raw.Map
		if _that, err = readGCSYaml(fmt.Sprintf("gs://nb-data/infra/asgard/clusters/%s.yaml", _globalOptions.Cluster)); err != nil {
			return
		}
		if _globalOptions.Values, err = raw.Get[map[string]any](_that, "global"); err != nil {
			return
		}
	})
	return
}
