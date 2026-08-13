package operation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/raw"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestShouldRenameHP(t *testing.T) {
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  template:
    spec:
      containers:
      - image: 'haha:1'`
	var err error
	var r k8s.Resource
	if r, err = k8s.DecodeYAML(stsYaml); err != nil {
		panic(err)
	}
	var sts *k8s.StatefulSet
	if sts, err = k8s.Parse[*k8s.StatefulSet](r); err != nil {
		panic(err)
	}
	should := shouldRename(sts)
	assert.True(t, should)
}

func TestShouldRenameWithAnnotation(t *testing.T) {
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
  annotations:
    'foreman/rotation': 'disabled'
spec:
  template:
    spec:
      containers:
      - image: 'haha:1'`
	var err error
	var r k8s.Resource
	if r, err = k8s.DecodeYAML(stsYaml); err != nil {
		panic(err)
	}
	var sts *k8s.StatefulSet
	if sts, err = k8s.Parse[*k8s.StatefulSet](r); err != nil {
		panic(err)
	}
	should := shouldRename(sts)
	assert.False(t, should)
}

func TestShouldRenameWithBlacklist(t *testing.T) {
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  template:
    spec:
      containers:
      - image: redis`
	var err error
	var r k8s.Resource
	if r, err = k8s.DecodeYAML(stsYaml); err != nil {
		panic(err)
	}
	var sts *k8s.StatefulSet
	if sts, err = k8s.Parse[*k8s.StatefulSet](r); err != nil {
		panic(err)
	}
	should := shouldRename(sts)
	assert.False(t, should)
}

func TestShouldRotateHP(t *testing.T) {
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 2
  template:
    spec:
      containers:
      - image: whocares`
	var err error
	var r k8s.Resource
	if r, err = k8s.DecodeYAML(stsYaml); err != nil {
		panic(err)
	}
	var sts *k8s.StatefulSet
	if sts, err = k8s.Parse[*k8s.StatefulSet](r); err != nil {
		panic(err)
	}
	df := raw.Map{
		"spec": raw.Map{
			"template": "whocares",
		},
	}
	should := shouldRotate(global.NewContext(context.Background()), df, sts)
	assert.False(t, should)
}

func TestShouldRotateSingleReplica(t *testing.T) {
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 1
  template:
    spec:
      containers:
      - image: whocares`
	var err error
	var r k8s.Resource
	if r, err = k8s.DecodeYAML(stsYaml); err != nil {
		panic(err)
	}
	var sts *k8s.StatefulSet
	if sts, err = k8s.Parse[*k8s.StatefulSet](r); err != nil {
		panic(err)
	}
	df := raw.Map{
		"spec": raw.Map{
			"template": "whocares",
		},
	}
	should := shouldRotate(global.NewContext(context.Background()), df, sts)
	assert.True(t, should)
}

func TestRotateStsHP(t *testing.T) {
	org := getCurrentRotation
	defer func() {
		getCurrentRotation = org
	}()
	getCurrentRotation = func(ctx context.Context, name, namespace string) *currentRotations {
		return &currentRotations{
			rotation: 2,
			names:    []string{"sts1---1"},
		}
	}
	var stsOld = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 2 
  template:
    spec:
      containers:
      - image: whocares`

	var stsNew = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 2 
  serviceName: whocares
  template:
    spec:
      containers:
      - image: whocares`
	var err error
	var old, new k8s.Resource
	if old, err = k8s.DecodeYAML(stsOld); err != nil {
		panic(err)
	}
	if new, err = k8s.DecodeYAML(stsNew); err != nil {
		panic(err)
	}
	var df raw.Map
	if df, err = raw.Diff(old, new); err != nil {
		df = nil
	}
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))

	toRemoves := []toRemove{}
	var rotated bool
	if rotated, err = rotateSts(rc, old, &new, &toRemoves, df); err != nil {
		panic(err)
	}
	assert.True(t, rotated)
	assert.Equal(t, "sts1---3", new.GetName())
}

func TestRotateStsNoRotation(t *testing.T) {
	org := getCurrentRotation
	defer func() {
		getCurrentRotation = org
	}()
	getCurrentRotation = func(ctx context.Context, name, namespace string) *currentRotations {
		return &currentRotations{
			rotation: 2,
			names:    []string{"sts1---1"},
		}
	}
	var stsOld = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 2 
  template:
    spec:
      containers:
      - image: whocares`

	var stsNew = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas:  3
  template:
    spec:
      containers:
      - image: whocares`
	var err error
	var old, new k8s.Resource
	if old, err = k8s.DecodeYAML(stsOld); err != nil {
		panic(err)
	}
	if new, err = k8s.DecodeYAML(stsNew); err != nil {
		panic(err)
	}
	var df raw.Map
	if df, err = raw.Diff(old, new); err != nil {
		df = nil
	}
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))
	toRemoves := []toRemove{}
	var rotated bool
	if rotated, err = rotateSts(rc, old, &new, &toRemoves, df); err != nil {
		panic(err)
	}
	assert.False(t, rotated)
	assert.Equal(t, "sts1---2", new.GetName())
}

func TestShouldRotateRegex(t *testing.T) {
	assert.False(t, stsRotationRegex.MatchString("mdm-pd-singapore-o6-1119503774d"))
	assert.True(t, stsRotationRegex.MatchString("mdm-pd-singapore-o6-1119503774d---0"))
}

func TestRenameStsIfNeeded(t *testing.T) {
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 2 
  template:
    spec:
      containers:
      - image: whocares`
	var err error
	var r k8s.Resource
	if r, err = k8s.DecodeYAML(stsYaml); err != nil {
		panic(err)
	}
	list := []k8s.Resource{r}
	nameMap := map[string]string{}
	renameStss(list, nameMap)
	r = list[0]
	assert.Equal(t, "sts1---0", r.GetName())
	assert.Equal(t, "sts1---0", nameMap["sts1"])
}

func TestApplyReportsCanonicalResourcesUpToFailure(t *testing.T) {
	// A StatefulSet applies fine, then the Service is rejected. The StatefulSet must be
	// reported under its canonical name, not the ---0 name renameStss gives it, because
	// that is how the manifest is keyed.
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 1
  template:
    spec:
      containers:
      - image: whocares`
	var svcYaml = `
kind: Service
metadata:
  name: svc1
spec:
  ports:
  - port: 8888`
	var err error
	var sts, svc k8s.Resource
	if sts, err = k8s.DecodeYAML(stsYaml); err != nil {
		panic(err)
	}
	if svc, err = k8s.DecodeYAML(svcYaml); err != nil {
		panic(err)
	}

	orgApplyResource := applyResource
	defer func() { applyResource = orgApplyResource }()
	applyResource = func(ctx context.Context, r k8s.Resource, options ...k8s.OperationOption) error {
		if r.GetObjectKind().GroupVersionKind().Kind == string(k8s.KindService) {
			return fmt.Errorf("admission webhook denied the request")
		}
		return nil
	}

	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))
	applied, err := apply(rc, []k8s.Resource{sts, svc}, nil, 0, map[string]bool{})
	assert.Error(t, err)
	assert.Len(t, applied, 1)
	assert.Equal(t, "sts1", applied[0].GetName())
}

func TestApplyRecordsNewResourceThatMayExistAfterWaitFailure(t *testing.T) {
	// k8s.Rollout creates before it waits, so with wait > 0 a failure can leave the
	// object behind. A brand new resource must still be recorded or it leaks.
	svc, err := k8s.DecodeYAML("kind: Service\nmetadata:\n  name: svc1\nspec:\n  ports:\n  - port: 1")
	if err != nil {
		panic(err)
	}
	orgApplyResource := applyResource
	defer func() { applyResource = orgApplyResource }()
	applyResource = func(ctx context.Context, r k8s.Resource, options ...k8s.OperationOption) error {
		return fmt.Errorf("timed out waiting for readiness")
	}
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))

	applied, err := apply(rc, []k8s.Resource{svc}, nil, time.Minute, map[string]bool{})
	assert.Error(t, err)
	assert.Len(t, applied, 1, "new resource must be recorded when a wait was requested")

	// With no wait there is no readiness phase, so a failure means nothing was created.
	applied, err = apply(rc, []k8s.Resource{svc}, nil, 0, map[string]bool{})
	assert.Error(t, err)
	assert.Len(t, applied, 0, "without a wait a failure means the object was not created")

	// A resource already in the recorded manifest stays at its old form so that a retry
	// still sees it as changed.
	applied, err = apply(rc, []k8s.Resource{svc}, nil, time.Minute, map[string]bool{
		resourceKey(string(k8s.KindService), "svc1"): true,
	})
	assert.Error(t, err)
	assert.Len(t, applied, 0, "known resources must not be overwritten with the new form")
}

func TestApplyWaitFailureUsesCanonicalNameForKnownStatefulSet(t *testing.T) {
	// `changed` is keyed by the canonical name, but the loop key is the post-renameStss
	// name. Looking up the renamed key would report an already-recorded StatefulSet as
	// new and overwrite its old recorded form.
	sts, err := k8s.DecodeYAML("kind: StatefulSet\nmetadata:\n  name: sts1\nspec:\n  replicas: 1\n  template:\n    spec:\n      containers:\n      - image: whocares")
	if err != nil {
		panic(err)
	}
	orgApplyResource := applyResource
	defer func() { applyResource = orgApplyResource }()
	applyResource = func(ctx context.Context, r k8s.Resource, options ...k8s.OperationOption) error {
		return fmt.Errorf("timed out waiting for readiness")
	}
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))

	applied, err := apply(rc, []k8s.Resource{sts}, nil, time.Minute, map[string]bool{
		resourceKey(string(k8s.KindStatefulSet), "sts1"): true,
	})
	assert.Error(t, err)
	assert.Len(t, applied, 0, "a StatefulSet already in the manifest must keep its old recorded form")
}

func TestMergeResourcesKeepsSameNameAcrossNamespaces(t *testing.T) {
	decode := func(y string) k8s.Resource {
		r, err := k8s.DecodeYAML(y)
		if err != nil {
			panic(err)
		}
		return r
	}
	// Same kind and name, different namespaces: collapsing them would drop one from the
	// rewritten manifest and leak it.
	svcA := decode("kind: Service\nmetadata:\n  name: shared\n  namespace: a")
	svcB := decode("kind: Service\nmetadata:\n  name: shared\n  namespace: b")

	merged := mergeResources([]k8s.Resource{svcA, svcB}, nil)
	assert.Len(t, merged, 2)
	assert.Equal(t, "a", merged[0].GetNamespace())
	assert.Equal(t, "b", merged[1].GetNamespace())
}

func TestMergeResourcesOverlaysAppliedOnOld(t *testing.T) {
	decode := func(y string) k8s.Resource {
		r, err := k8s.DecodeYAML(y)
		if err != nil {
			panic(err)
		}
		return r
	}
	oldSvc := decode("kind: Service\nmetadata:\n  name: svc1\nspec:\n  ports:\n  - port: 1")
	newSvc := decode("kind: Service\nmetadata:\n  name: svc1\nspec:\n  ports:\n  - port: 2")
	oldCm := decode("kind: ConfigMap\nmetadata:\n  name: cm1\ndata:\n  a: b")

	merged := mergeResources([]k8s.Resource{oldSvc, oldCm}, []k8s.Resource{newSvc})
	// The applied Service replaces the old one; the untouched ConfigMap is retained so
	// uninstall still removes it.
	assert.Len(t, merged, 2)
	assert.Equal(t, "svc1", merged[0].GetName())
	assert.Equal(t, "cm1", merged[1].GetName())
	ports, err := raw.ChainGet[[]any](merged[0].(*unstructured.Unstructured).UnstructuredContent(), "spec", "ports")
	assert.NoError(t, err)
	assert.Len(t, ports, 1)
	assert.Equal(t, int64(2), ports[0].(map[string]any)["port"])
}

func TestEncodeResourcesDoesNotInjectDefaults(t *testing.T) {
	// Encoding through typed structs would add spec.template.metadata.creationTimestamp
	// and status, which read back as spurious diffs and can trigger an unwanted rotation.
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 1
  template:
    spec:
      containers:
      - image: whocares`
	r, err := k8s.DecodeYAML(stsYaml)
	if err != nil {
		panic(err)
	}
	encoded, err := encodeResources([]k8s.Resource{r})
	assert.NoError(t, err)
	assert.NotContains(t, encoded, "creationTimestamp")
	assert.NotContains(t, encoded, "status:")

	// and it must round-trip through the same decoder Remove/getManifests use
	back, err := k8s.DecodeAllYAML(encoded)
	assert.NoError(t, err)
	assert.Len(t, back, 1)
	assert.Equal(t, "sts1", back[0].GetName())
	assert.Equal(t, string(k8s.KindStatefulSet), back[0].GetObjectKind().GroupVersionKind().Kind)
}

func TestRecordPartialManifestWritesUnderReleaseName(t *testing.T) {
	decode := func(y string) k8s.Resource {
		r, err := k8s.DecodeYAML(y)
		if err != nil {
			panic(err)
		}
		return r
	}
	oldCm := decode("kind: ConfigMap\nmetadata:\n  name: cm1\ndata:\n  a: b")
	appliedSvc := decode("kind: Service\nmetadata:\n  name: svc1\nspec:\n  ports:\n  - port: 8888")

	orgWriteManifest := writeManifest
	defer func() { writeManifest = orgWriteManifest }()
	var gotName, gotValue string
	writeManifest = func(ctx context.Context, value, name, namespace string) error {
		gotName, gotValue = name, value
		return nil
	}

	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))
	recordPartialManifest(rc, []k8s.Resource{oldCm}, []k8s.Resource{appliedSvc}, "release1")

	assert.Equal(t, "release1", gotName)
	written, err := k8s.DecodeAllYAML(gotValue)
	assert.NoError(t, err)
	assert.Len(t, written, 2)
	assert.Equal(t, "cm1", written[0].GetName())
	assert.Equal(t, "svc1", written[1].GetName())
}

func TestRecordPartialManifestSkipsWhenNothingToRecord(t *testing.T) {
	orgWriteManifest := writeManifest
	defer func() { writeManifest = orgWriteManifest }()
	var called bool
	writeManifest = func(ctx context.Context, value, name, namespace string) error {
		called = true
		return nil
	}
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))
	recordPartialManifest(rc, nil, nil, "release1")
	assert.False(t, called)
}

func TestRemove(t *testing.T) {
	var stsYaml = `
kind: StatefulSet
metadata:
  name: sts1
spec:
  replicas: 2 
  template:
    spec:
      containers:
      - image: whocares`
	var err error
	var r k8s.Resource
	if r, err = k8s.DecodeYAML(stsYaml); err != nil {
		panic(err)
	}
	orgGetCurrentRotation := getCurrentRotation
	orgGetExistingManifest := getExistingManifest
	orgDoRemove := doRemove
	defer func() {
		getCurrentRotation = orgGetCurrentRotation
		getExistingManifest = orgGetExistingManifest
		doRemove = orgDoRemove
	}()
	getCurrentRotation = func(ctx context.Context, name, namespace string) *currentRotations {
		return &currentRotations{
			rotation: 0,
			names:    []string{"sts1---0"},
		}
	}

	getExistingManifest = func(ctx context.Context, name, namespace string) (existing []k8s.Resource, err error) {
		return []k8s.Resource{r}, nil
	}
	removed := map[string]bool{}
	doRemove = func(ctx context.Context, name, namespace string, kind k8s.Kind, options ...k8s.OperationOption) error {
		//println("doRemove mock: ", name)
		removed[name] = true
		return nil
	}

	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))
	if err = Remove(rc, "sts1", "whocares"); err != nil {
		t.Fatal(err)
	}
	assert.True(t, removed["sts1---0"])
	assert.True(t, removed["sts1-manifest"])
}
