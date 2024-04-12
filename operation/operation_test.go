package operation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/raw"
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
	should := shouldRotate(df, sts)
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
	should := shouldRotate(df, sts)
	assert.True(t, should)
}

func TestRotateStsHP(t *testing.T) {
	org := getCurrentRotation
	defer func() {
		getCurrentRotation = org
	}()
	getCurrentRotation = func(ctx context.Context, sts *k8s.StatefulSet) *currentRotations {
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

	toRemoves := []toRemove{}
	var rotated bool
	if rotated, err = rotateSts(context.Background(), old, &new, &toRemoves, df); err != nil {
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
	getCurrentRotation = func(ctx context.Context, sts *k8s.StatefulSet) *currentRotations {
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

	toRemoves := []toRemove{}
	var rotated bool
	if rotated, err = rotateSts(context.Background(), old, &new, &toRemoves, df); err != nil {
		panic(err)
	}
	assert.False(t, rotated)
	assert.Equal(t, "sts1---2", new.GetName())
}
