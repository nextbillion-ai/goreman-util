package global

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/nextbillion-ai/gsg/lib/object"
	"github.com/zhchang/goquiver/raw"
	"gopkg.in/yaml.v3"
)

var globalSpecOnce sync.Once
var globalSpec map[string]any

// GlobalSpec retrieves the global specification for a given cluster.
// It reads the specification from a YAML file located in a Google Cloud Storage bucket.
// The cluster parameter specifies the name of the cluster.
// The function returns a map[string]any containing the global specification and an error, if any.
func GlobalSpec(cluster string) (map[string]any, error) {
	var err error
	globalSpecOnce.Do(func() {
		var o *object.Object
		if o, err = object.New(fmt.Sprintf("gs://nb-data/infra/asgard/clusters/%s.yaml", cluster)); err != nil {
			return
		}
		var buf bytes.Buffer
		if err = o.Read(&buf); err != nil {
			return
		}
		_that := map[string]any{}
		if err = yaml.Unmarshal(buf.Bytes(), _that); err != nil {
			return
		}
		if globalSpec, err = raw.Get[map[string]any](_that, "global"); err != nil {
			return
		}
	})
	return globalSpec, err
}
