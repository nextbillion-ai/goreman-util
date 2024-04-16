package global

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhchang/goquiver/raw"
)

func TestGlobalSpec(t *testing.T) {
	readGCSYamlOrg := readGCSYaml
	_globalOptionsOrg := _globalOptions
	defer func() {
		readGCSYaml = readGCSYamlOrg
		_globalOptions = _globalOptionsOrg
	}()
	var gcsMock = raw.Map{
		"mockkey1": "mockvalue1",
		"mockkey2": "mockvalue2",
	}
	readGCSYaml = func(url string) (raw.Map, error) {
		return gcsMock, nil
	}
	_globalOptions = &Options{Values: raw.Map{}}

	rc := NewContext(context.Background(), WithNamespace("mock-ns"), WithPlugins([]*Plugin{
		{
			Name: "mock-plugin-name",
			Url:  "{namespace}-{cluster}-{name}",
			Keys: []string{"mockkey1", "mockkey2"},
		},
	}))
	var spec raw.Map
	var err error
	if spec, err = GlobalSpec(rc, "whocares", raw.Map{}); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, gcsMock, spec["mock-plugin-name"])
}
