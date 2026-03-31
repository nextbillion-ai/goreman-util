package global

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhchang/goquiver/raw"
)

func setupGlobalTest(t *testing.T) func() {
	readGCSYamlOrg := readGCSYaml
	_globalOptionsOrg := _globalOptions
	return func() {
		readGCSYaml = readGCSYamlOrg
		_globalOptions = _globalOptionsOrg
	}
}

func TestGlobalSpec(t *testing.T) {
	defer setupGlobalTest(t)()
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

func TestGlobalSpec_PluginUrlNotMutated(t *testing.T) {
	defer setupGlobalTest(t)()
	readGCSYaml = func(url string) (raw.Map, error) {
		return raw.Map{"k": "v"}, nil
	}
	_globalOptions = &Options{Cluster: "prod", Values: raw.Map{}}

	plugin := &Plugin{
		Name: "p",
		Url:  "gs://bucket/{cluster}/{namespace}/{name}.yaml",
		Keys: []string{"k"},
	}
	rc := NewContext(context.Background(), WithNamespace("ns1"), WithPlugins([]*Plugin{plugin}))

	_, err := GlobalSpec(rc, "app1", raw.Map{})
	assert.NoError(t, err)
	assert.Equal(t, "gs://bucket/{cluster}/{namespace}/{name}.yaml", plugin.Url,
		"plugin.Url should not be mutated after GlobalSpec call")

	// calling a second time with different name should still produce correct URL
	var capturedUrl string
	readGCSYaml = func(url string) (raw.Map, error) {
		capturedUrl = url
		return raw.Map{"k": "v"}, nil
	}
	_, err = GlobalSpec(rc, "app2", raw.Map{})
	assert.NoError(t, err)
	assert.Equal(t, "gs://bucket/prod/ns1/app2.yaml", capturedUrl,
		"second call should resolve placeholders from original template, not the already-replaced URL")
}

func TestGlobalSpec_PlaceholderMatchesBracesOnly(t *testing.T) {
	defer setupGlobalTest(t)()
	readGCSYaml = func(url string) (raw.Map, error) {
		return raw.Map{"k": "v"}, nil
	}
	_globalOptions = &Options{Cluster: "prod", Values: raw.Map{}}

	// URL contains "area" and "mode" as literal path segments, not as {area}/{mode} placeholders.
	// This should NOT trigger the appValue lookup.
	plugin := &Plugin{
		Name: "p",
		Url:  "gs://bucket/area/mode/context/config.yaml",
		Keys: []string{"k"},
	}
	rc := NewContext(context.Background(), WithNamespace("ns"), WithPlugins([]*Plugin{plugin}))

	spec, err := GlobalSpec(rc, "myapp", raw.Map{})
	assert.NoError(t, err, "should not fail even though appValue has no 'area'/'mode'/'context' keys")
	assert.Equal(t, raw.Map{"k": "v"}, spec["p"])
}

func TestGlobalSpec_PlaceholderAreaModeContext(t *testing.T) {
	defer setupGlobalTest(t)()
	var capturedUrl string
	readGCSYaml = func(url string) (raw.Map, error) {
		capturedUrl = url
		return raw.Map{"k": "v"}, nil
	}
	_globalOptions = &Options{Cluster: "staging", Values: raw.Map{}}

	plugin := &Plugin{
		Name: "p",
		Url:  "gs://bucket/{cluster}/{area}/{mode}/{context}.yaml",
		Keys: []string{"k"},
	}
	rc := NewContext(context.Background(), WithNamespace("ns"), WithPlugins([]*Plugin{plugin}))

	appValue := raw.Map{
		"area":    "ap-southeast",
		"mode":    "driving",
		"context": "routing",
	}
	_, err := GlobalSpec(rc, "myapp", appValue)
	assert.NoError(t, err)
	assert.Equal(t, "gs://bucket/staging/ap-southeast/driving/routing.yaml", capturedUrl)
}

func TestGlobalSpec_SkipsPluginWithEmptyFields(t *testing.T) {
	defer setupGlobalTest(t)()
	readGCSYaml = func(url string) (raw.Map, error) {
		t.Fatal("readGCSYaml should not be called for skipped plugins")
		return nil, nil
	}
	_globalOptions = &Options{Values: raw.Map{}}

	cases := []struct {
		desc   string
		plugin *Plugin
	}{
		{"empty name", &Plugin{Name: "", Url: "gs://x", Keys: []string{"k"}}},
		{"empty url", &Plugin{Name: "p", Url: "", Keys: []string{"k"}}},
		{"empty keys", &Plugin{Name: "p", Url: "gs://x", Keys: []string{}}},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			rc := NewContext(context.Background(), WithPlugins([]*Plugin{tc.plugin}))
			spec, err := GlobalSpec(rc, "app", raw.Map{})
			assert.NoError(t, err)
			assert.Nil(t, spec[tc.plugin.Name])
		})
	}
}
