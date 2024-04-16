package global

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhchang/goquiver/raw"
)

func TestGlobalSpec(t *testing.T) {
	readGCSYamlOrg := readGCSYaml
	globalValuesOrg := globalValues
	defer func() {
		readGCSYaml = readGCSYamlOrg
		globalValues = globalValuesOrg
	}()
	var gcsMock = raw.Map{
		"mockkey1": "mockvalue1",
		"mockkey2": "mockvalue2",
	}
	readGCSYaml = func(url string) (raw.Map, error) {
		return gcsMock, nil
	}
	globalValues = func(cluster string) (raw.Map, error) {
		return raw.Map{}, nil
	}

	rc := NewContext(context.Background(), WithNamespace("mock-ns"), WithCluster("mock-cluster"), WithPlugins([]*Plugin{
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
