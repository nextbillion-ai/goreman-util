package operation

import (
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
