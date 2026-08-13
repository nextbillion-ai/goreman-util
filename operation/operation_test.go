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
	applied, _, err := apply(rc, []k8s.Resource{sts, svc}, nil, 0, map[string]bool{})
	assert.Error(t, err)
	assert.Len(t, applied, 1)
	assert.Equal(t, "sts1", applied[0].GetName())
}

func TestApplyNeverRecordsAFailedResource(t *testing.T) {
	// Recording a resource we did not manage to apply would make the next rollout diff
	// it against itself, skip it, and report success while it stays missing. That holds
	// whether or not a readiness wait was requested.
	svc, err := k8s.DecodeYAML("kind: Service\nmetadata:\n  name: svc1\nspec:\n  ports:\n  - port: 1")
	if err != nil {
		panic(err)
	}
	orgApplyResource := applyResource
	defer func() { applyResource = orgApplyResource }()
	applyResource = func(ctx context.Context, r k8s.Resource, options ...k8s.OperationOption) error {
		return fmt.Errorf("admission webhook denied the request")
	}
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))

	for _, wait := range []time.Duration{0, time.Minute} {
		applied, _, err := apply(rc, []k8s.Resource{svc}, nil, wait, map[string]bool{})
		assert.Error(t, err)
		assert.Len(t, applied, 0, "a resource that failed to apply must not be recorded")
	}
}

func TestRecordPartialManifestSkipsContentThatCannotRoundTrip(t *testing.T) {
	// Exercises the guard with content that provably does not survive the round trip:
	// DecodeAllYAML splits on the substring "---", so a PEM block does not come back.
	// Such a chart would already fail earlier in GenManifest, which decodes helm's
	// output, so this is the guard working rather than a reachable production input.
	cm, err := k8s.DecodeYAML("kind: ConfigMap\nmetadata:\n  name: cm1\ndata:\n  cert: |\n    -----BEGIN CERTIFICATE-----\n    abc\n    -----END CERTIFICATE-----\n")
	if err != nil {
		panic(err)
	}
	orgWriteManifest := writeManifest
	defer func() { writeManifest = orgWriteManifest }()
	var called bool
	writeManifest = func(ctx context.Context, value, name, namespace string) error {
		called = true
		return nil
	}
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))
	recordPartialManifest(rc, nil, []k8s.Resource{cm}, "release1")
	assert.False(t, called, "must not overwrite the existing manifest with unreadable content")
}

func TestRecordPartialManifestWritesEvenWhenContextIsCancelled(t *testing.T) {
	// A cancelled or timed out context is itself a reason a rollout fails, and that is
	// exactly when the record matters. The write must not inherit the cancellation.
	svc, err := k8s.DecodeYAML("kind: Service\nmetadata:\n  name: svc1")
	if err != nil {
		panic(err)
	}
	orgWriteManifest := writeManifest
	defer func() { writeManifest = orgWriteManifest }()
	var gotErr error
	writeManifest = func(ctx context.Context, value, name, namespace string) error {
		gotErr = ctx.Err()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rc := global.NewContext(ctx, global.WithLogLevel(logrus.ErrorLevel))
	recordPartialManifest(rc, nil, []k8s.Resource{svc}, "release1")

	assert.NoError(t, gotErr, "the write context must not carry the rollout's cancellation")
}

func TestApplyReportsFailedRemovals(t *testing.T) {
	// A resource dropped from the chart whose deletion fails is still running. It has to
	// be reported, or the caller writes a manifest that no longer mentions it and
	// nothing ever deletes it again.
	orgDoRemove := doRemove
	defer func() { doRemove = orgDoRemove }()
	doRemove = func(ctx context.Context, name, namespace string, kind k8s.Kind, options ...k8s.OperationOption) error {
		if name == "stuck" {
			return fmt.Errorf("object has a finalizer")
		}
		return nil
	}
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))

	removals := []toRemove{
		{name: "gone", namespace: "ns", kind: k8s.KindService},
		{name: "stuck", namespace: "ns", kind: k8s.KindConfigMap},
	}
	applied, failed, err := apply(rc, nil, removals, 0, map[string]bool{})
	assert.NoError(t, err, "a failed removal must not fail the rollout")
	assert.Empty(t, applied)
	assert.Len(t, failed, 1)
	assert.Equal(t, "stuck", failed[0].name)
}

func TestFinalManifest(t *testing.T) {
	decode := func(y string) k8s.Resource {
		r, err := k8s.DecodeYAML(y)
		if err != nil {
			panic(err)
		}
		return r
	}
	svc := decode("kind: Service\nmetadata:\n  name: svc1\n  namespace: ns")
	stuck := decode("kind: ConfigMap\nmetadata:\n  name: stuck\n  namespace: ns")
	rc := global.NewContext(context.Background(), global.WithLogLevel(logrus.ErrorLevel))

	// nothing failed: the rendered manifest is stored verbatim
	got := finalManifest(rc, []k8s.Resource{svc, stuck}, []k8s.Resource{svc}, nil, "RENDERED", "release1")
	assert.Equal(t, "RENDERED", got)

	// a removal failed: the resource is merged back in so it stays tracked
	got = finalManifest(rc, []k8s.Resource{svc, stuck}, []k8s.Resource{svc},
		[]toRemove{{name: "stuck", namespace: "ns", kind: k8s.KindConfigMap}}, "RENDERED", "release1")
	assert.NotEqual(t, "RENDERED", got)
	back, err := k8s.DecodeAllYAML(got)
	assert.NoError(t, err)
	assert.Len(t, back, 2)
	assert.Equal(t, "svc1", back[0].GetName())
	assert.Equal(t, "stuck", back[1].GetName())

	// a removal that matches nothing recorded leaves the rendered manifest alone
	got = finalManifest(rc, []k8s.Resource{svc}, []k8s.Resource{svc},
		[]toRemove{{name: "sts1---0", namespace: "ns", kind: k8s.KindStatefulSet}}, "RENDERED", "release1")
	assert.Equal(t, "RENDERED", got)
}

func TestResourcesForMatchesRemovalsByIdentity(t *testing.T) {
	decode := func(y string) k8s.Resource {
		r, err := k8s.DecodeYAML(y)
		if err != nil {
			panic(err)
		}
		return r
	}
	cm := decode("kind: ConfigMap\nmetadata:\n  name: stuck\n  namespace: ns")
	svc := decode("kind: Service\nmetadata:\n  name: other\n  namespace: ns")

	kept := resourcesFor([]k8s.Resource{cm, svc}, []toRemove{
		{name: "stuck", namespace: "ns", kind: k8s.KindConfigMap},
	})
	assert.Len(t, kept, 1)
	assert.Equal(t, "stuck", kept[0].GetName())

	// a same-named resource of a different kind must not match
	kept = resourcesFor([]k8s.Resource{cm}, []toRemove{
		{name: "stuck", namespace: "ns", kind: k8s.KindService},
	})
	assert.Empty(t, kept)

	// a rotation-suffixed removal has no manifest entry, so it matches nothing
	kept = resourcesFor([]k8s.Resource{cm}, []toRemove{
		{name: "stuck---0", namespace: "ns", kind: k8s.KindStatefulSet},
	})
	assert.Empty(t, kept)
}

func TestEncodeManifestRejectsContentThatCannotRoundTrip(t *testing.T) {
	cm, err := k8s.DecodeYAML("kind: ConfigMap\nmetadata:\n  name: cm1\ndata:\n  cert: |\n    -----BEGIN CERTIFICATE-----\n    abc\n    -----END CERTIFICATE-----\n")
	if err != nil {
		panic(err)
	}
	_, err = encodeManifest([]k8s.Resource{cm})
	assert.Error(t, err)

	svc, err := k8s.DecodeYAML("kind: Service\nmetadata:\n  name: svc1")
	if err != nil {
		panic(err)
	}
	value, err := encodeManifest([]k8s.Resource{svc})
	assert.NoError(t, err)
	assert.Contains(t, value, "svc1")
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
