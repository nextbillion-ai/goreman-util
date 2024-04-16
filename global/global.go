package global

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/nextbillion-ai/gsg/lib/object"
	"github.com/zhchang/goquiver/raw"
	"gopkg.in/yaml.v3"
)

var globalSpecOnce sync.Once
var _globalValues map[string]any

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

// GlobalSpec retrieves the global specification for a given cluster.
// It reads the specification from a YAML file located in a Google Cloud Storage bucket.
// The cluster parameter specifies the name of the cluster.
// The function returns a map[string]any containing the global specification and an error, if any.
var globalValues = func(cluster string) (map[string]any, error) {
	var err error
	globalSpecOnce.Do(func() {
		var _that raw.Map
		if _that, err = readGCSYaml(fmt.Sprintf("gs://nb-data/infra/asgard/clusters/%s.yaml", cluster)); err != nil {
			return
		}
		if _globalValues, err = raw.Get[map[string]any](_that, "global"); err != nil {
			return
		}
	})
	return _globalValues, err
}

var GlobalSpec = func(rc ResourceContext, name string, appValue raw.Map) (spec raw.Map, err error) {
	if spec, err = globalValues(rc.Cluster()); err != nil {
		return
	}
	for _, plugin := range rc.Plugins() {
		if plugin.Name == "" || plugin.Url == "" || len(plugin.Keys) == 0 {
			rc.Logger().Warnf("plugin %s not loaded because its name/url/keys are empty", plugin.Name)
			continue
		}
		plugin.Url = strings.ReplaceAll(plugin.Url, `{cluster}`, rc.Cluster())
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
