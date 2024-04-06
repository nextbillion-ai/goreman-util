package global

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/nextbillion-ai/gsg/lib/object"
	"github.com/zhchang/goquiver/raw"
	"gopkg.in/yaml.v3"
)

type Plugin struct {
	Name string
	Keys []string
	Url  string
}

var globalSpecOnce sync.Once
var globalSpec map[string]any

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
