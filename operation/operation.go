// Package operation provides utilities for managing and manipulating Kubernetes resources.
// It includes functionalities for handling Kubernetes operations such as getting existing
// manifests, and managing resources to be removed.
//
// The package uses the "github.com/nextbillion-ai/goreman-util/global" library for global
// configurations and specifications, "github.com/zhchang/goquiver/k8s" for Kubernetes
// operations, and "github.com/zhchang/goquiver/raw" for raw data operations.
package operation

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/raw"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type toRemove struct {
	name      string
	namespace string
	kind      k8s.Kind
}

func resourceKey(kind, name string) string {
	return kind + name
}

var getExistingManifest = func(ctx context.Context, name, namespace string) (existing []k8s.Resource, err error) {
	var _t []k8s.Resource
	var cfg *k8s.ConfigMap
	if cfg, err = k8s.Get[*k8s.ConfigMap](ctx, name+"-manifest", namespace); err != nil {
		return
	}
	if _t, err = k8s.DecodeAllYAML(cfg.Data["manifest"]); err == nil {
		existing = _t
		return
	}
	return
}

func getManifests(ctx context.Context, chartPath string, values raw.Map) (old []k8s.Resource, new []k8s.Resource, newMap map[string]*k8s.Resource, newStr string, err error) {
	if new, newStr, err = k8s.GenManifest(ctx, chartPath, values); err != nil {
		return
	}
	var name, namespace string
	if name, err = raw.ChainGet[string](values, "global", "name"); err != nil {
		return
	}
	if namespace, err = raw.ChainGet[string](values, "global", "namespace"); err != nil {
		return
	}
	if old, err = getExistingManifest(ctx, name, namespace); err != nil {
		old = nil
	}
	newMap = map[string]*k8s.Resource{}
	for _, r := range new {
		newMap[resourceKey(r.GetObjectKind().GroupVersionKind().Kind, r.GetName())] = &r
	}
	err = nil
	return
}

var blackLists = []*regexp.Regexp{regexp.MustCompile(`^(docker.io\/)*redis`), regexp.MustCompile(`^(docker.io\/)*postgres`)}

func shouldRename(sts *k8s.StatefulSet) bool {
	var rotationFlag = ""
	if sts.ObjectMeta.Annotations != nil {
		rotationFlag = sts.ObjectMeta.Annotations["foreman/rotation"]
	}
	var hasRotationBlacklist = false
	for _, c := range sts.Spec.Template.Spec.Containers {
		for _, bl := range blackLists {
			if bl.MatchString(c.Image) {
				hasRotationBlacklist = true
				break
			}
		}
		if hasRotationBlacklist {
			break
		}
	}
	if rotationFlag == "" {
		if hasRotationBlacklist {
			rotationFlag = "disabled"
		} else {
			rotationFlag = "enabled"
		}
	}
	return rotationFlag == "enabled"
}

var rotatekeys = map[string]struct{}{
	"template":       {},
	"replicas":       {},
	"updateStrategy": {},
}

func shouldRotate(rc global.ResourceContext, df raw.Map, sts *k8s.StatefulSet) bool {
	if len(df) == 0 {
		return false
	}
	var err error
	var specMap raw.Map
	if specMap, err = raw.Get[raw.Map](df, "spec"); err != nil {
		return false
	}
	if !shouldRename(sts) {
		return false
	}
	specChanges := make([]string, 0, len(specMap))
	for k := range specMap {
		specChanges = append(specChanges, k)
	}
	if sts.Spec.Replicas != nil && *sts.Spec.Replicas == 1 {
		if len(specChanges) == 1 && specChanges[0] == "replicas" {
			return false
		}
		rc.Logger().Debugf("[rotate reason] spec changes with 1 replica sts: %+v", specChanges)
		return true
	}
	for _, key := range specChanges {
		if _, ok := rotatekeys[key]; !ok {
			rc.Logger().Debugf("[rotate reason] must rotate spec change: %+v", key)
			return true
		}
	}
	var templateMap raw.Map
	if templateMap, err = raw.ChainGet[raw.Map](df, "spec", "template"); err == nil {
		if _, ok := templateMap["labels"]; ok {
			rc.Logger().Debugf("[rotate reason] spec template labels changed")
			return true
		}
	}
	return false
}

type currentRotations struct {
	names    []string
	rotation int
}

var stsRotationRegex = regexp.MustCompile(`.*---(\d+)`)

func extractRotation(name string) (rotation int, err error) {
	// Create a regular expression to find digits following the pattern .*---\d+
	// Find the first submatch, which will include the entire match and the captured groups
	matches := stsRotationRegex.FindStringSubmatch(name)
	if matches == nil || len(matches) < 2 {
		err = fmt.Errorf("rotation not found")
		return
	}
	rotation, err = strconv.Atoi(matches[1])
	return
}

var getCurrentRotation = func(ctx context.Context, name, namespace string) *currentRotations {
	var stss []*k8s.StatefulSet
	var err error
	if stss, err = k8s.List[*k8s.StatefulSet](ctx, namespace, k8s.WithRegex(regexp.MustCompile(fmt.Sprintf(`%s---\d+`, name)))); err != nil {
		return nil
	}
	if len(stss) == 0 {
		return nil
	}
	slices.SortFunc[[]*k8s.StatefulSet](stss, func(a, b *k8s.StatefulSet) int {
		return cmp.Compare[string](a.GetName(), b.GetName())
	})
	var rotation int
	if rotation, err = extractRotation(stss[len(stss)-1].GetName()); err != nil {
		return nil
	}

	r := &currentRotations{
		rotation: rotation,
	}
	for _, sts := range stss {
		r.names = append(r.names, sts.GetName())
	}
	return r
}

const realNameLabel = "app.kubernetes.io/realname"

func setLabel(meta *metav1.ObjectMeta, key, value string) {
	if meta.Labels == nil {
		meta.Labels = map[string]string{}
	}
	meta.Labels[key] = value
}

func getLabel(meta *metav1.ObjectMeta, key string) string {
	if meta.Labels == nil {
		return ""
	}
	return meta.Labels[key]
}

func rotateSts(rc global.ResourceContext, old k8s.Resource, new *k8s.Resource, toRemoves *[]toRemove, df raw.Map) (rotated bool, err error) {
	//var changed = len(df) > 0
	var sts *k8s.StatefulSet
	if sts, err = k8s.Parse[*k8s.StatefulSet](old); err != nil {
		return
	}
	sr := len(df) > 0 && shouldRotate(rc, df, sts)
	rc.Logger().Infof("trying to rotate manifest for %s/%s", sts.GetNamespace(), sts.GetName())
	var current = getCurrentRotation(rc.Context(), sts.Name, rc.Namespace())
	if current != nil {
		rc.Logger().Infof(`current rotation for %s/%s is %d`, sts.GetNamespace(), sts.GetName(), current.rotation)
	}
	var removeAll bool
	var newStsName string = sts.GetName()
	if sr {
		if current == nil {
			err = fmt.Errorf("no current rotation found.")
			return
		}
		removeAll = true
		newStsName = sts.GetName() + `---` + strconv.Itoa(current.rotation+1)
		rotated = true
	} else if current != nil {
		newStsName = sts.GetName() + `---` + strconv.Itoa(current.rotation)
	}
	var newSts *k8s.StatefulSet
	if newSts, err = k8s.Parse[*k8s.StatefulSet](*new); err != nil {
		return
	}
	newSts.ObjectMeta.Name = newStsName
	setLabel(&newSts.ObjectMeta, realNameLabel, newStsName)
	setLabel(&newSts.Spec.Template.ObjectMeta, realNameLabel, newStsName)
	if len(newSts.Spec.Template.Spec.TopologySpreadConstraints) > 0 {
		for _, tsc := range newSts.Spec.Template.Spec.TopologySpreadConstraints {
			tsc.LabelSelector.MatchLabels = map[string]string{
				realNameLabel: newStsName,
			}
		}
	}
	if current != nil {
		var removes []string = current.names[:len(current.names)-1]
		if removeAll {
			removes = current.names
		}
		ns := newSts.GetNamespace()
		for _, remove := range removes {
			*toRemoves = append(*toRemoves, toRemove{name: remove, namespace: ns, kind: k8s.KindStatefulSet})
		}
	}
	rc.Logger().Infof(`applying rotation: %s`, newSts.GetName())
	*new = newSts
	err = nil
	return
}

type operationOptions struct {
	wait time.Duration
}

type OperationOption func(*operationOptions)

// WithWait sets the wait duration for an operation.
// The wait duration specifies how long the operation should wait before timing out.
// It returns an OperationOption that can be used to configure the operation.
func WithWait(d time.Duration) OperationOption {
	return func(opts *operationOptions) {
		opts.wait = d
	}
}

// Rollout applies a rolling update to the Kubernetes resources defined in the specified chart.
// It compares the existing resources with the new resources and performs necessary updates.
// The function takes a resource context, chart path, values, and optional operation options as parameters.
// It returns an error if any error occurs during the rollout process.
func Rollout(rc global.ResourceContext, chartPath string, values raw.Map, options ...OperationOption) error {
	opts := &operationOptions{}
	for _, opt := range options {
		opt(opts)
	}
	var err error
	var old, new []k8s.Resource
	var newMap map[string]*k8s.Resource
	var newStr string
	if old, new, newMap, newStr, err = getManifests(rc.Context(), chartPath, values); err != nil {
		return err
	}
	if len(new) == 0 {
		return fmt.Errorf("nothing to rollout")
	}

	var toRemoves []toRemove
	var changed = map[string]bool{}

	var rotated bool
	for _, r := range old {
		kind := r.GetObjectKind().GroupVersionKind().Kind
		key := resourceKey(kind, r.GetName())
		var ok bool
		var nr *k8s.Resource
		if nr, ok = newMap[key]; !ok {
			toRemoves = append(toRemoves, toRemove{name: r.GetName(), namespace: r.GetNamespace(), kind: kind})
			continue
		}
		var df raw.Map
		if df, err = raw.Diff(r, *nr); err != nil {
			df = nil
		}
		changed[key] = len(df) > 0
		rc.Logger().Debugf("%s changed: %t", key, changed[key])
		if kind == k8s.KindStatefulSet {
			var _rotated bool
			if _rotated, err = rotateSts(rc, r, nr, &toRemoves, df); err != nil {
				return err
			}
			if !rotated && _rotated {
				rotated = _rotated
			}
		}
	}
	if !rotated {
		opts.wait = time.Duration(0)
	}
	if err = apply(rc, new, toRemoves, opts.wait, changed); err != nil {
		return err
	}
	return writeManifest(rc.Context(), newStr, new[0].GetName(), rc.Namespace())
}

func writeManifest(ctx context.Context, value, name, namespace string) error {

	var manifest k8s.ConfigMap
	manifest.Kind = k8s.KindConfigMap
	manifest.ObjectMeta.Name = name + "-manifest"
	manifest.ObjectMeta.Namespace = namespace
	manifest.Data = map[string]string{
		"manifest": value,
	}
	return k8s.Rollout(ctx, &manifest)
}

func renameStss(list []k8s.Resource, stsNameToRealName map[string]string) {
	for index, r := range list {
		kind := r.GetObjectKind().GroupVersionKind().Kind
		if kind == k8s.KindStatefulSet {
			//var sts *k8s.StatefulSet
			sts, _ := k8s.Parse[*k8s.StatefulSet](r)
			if !stsRotationRegex.MatchString(r.GetName()) && shouldRename(sts) {
				org := sts.GetName()
				sts.ObjectMeta.Name += "---0"
				list[index] = sts
				stsNameToRealName[org] = sts.ObjectMeta.Name
			} else {
				org := getLabel(&sts.ObjectMeta, "app.kubernetes.io/name")
				stsNameToRealName[org] = getLabel(&sts.ObjectMeta, "app.kubernetes.io/realname")
			}
		}
	}
}

func apply(rc global.ResourceContext, new []k8s.Resource, toRemoves []toRemove, wait time.Duration, changed map[string]bool) (err error) {
	stsNameToRealName := map[string]string{}
	renameStss(new, stsNameToRealName)

	for _, r := range new {
		kind := r.GetObjectKind().GroupVersionKind().Kind
		key := resourceKey(kind, r.GetName())
		if kind == k8s.KindHorizontalPodAutoscaler {
			var hpa *k8s.HorizontalPodAutoscaler
			hpa, _ = k8s.Parse[*k8s.HorizontalPodAutoscaler](r)
			if hpa.Spec.ScaleTargetRef.Kind == k8s.KindStatefulSet {
				targetStsName := hpa.Spec.ScaleTargetRef.Name
				var exists bool
				var realName string
				if realName, exists = stsNameToRealName[targetStsName]; exists {
					hpa.Spec.ScaleTargetRef.Name = realName
					changed[key] = true
				}

			}
		}
		var didChange, exists bool
		if didChange, exists = changed[key]; exists && !didChange {
			rc.Logger().Infof(`applyManifest skipped for item: %s`, key)
			continue
		}
		rc.Logger().Debugf(`applyManifest going for item: %s,conditons: %t,%t`, key, didChange, exists)
		if err = k8s.Rollout(rc.Context(), r, k8s.WithWait(wait)); err != nil {
			return
		}
	}

	for _, r := range toRemoves {
		if err = k8s.Remove(rc.Context(), r.name, r.namespace, r.kind, k8s.WithWait(2*time.Minute)); err != nil {
			rc.Logger().Warnf("failed to remove %s-%s/%s: %s", r.kind, r.namespace, r.name, err)
		}
	}
	err = nil
	return
}

var doRemove = func(ctx context.Context, name, namespace string, kind k8s.Kind, options ...k8s.OperationOption) error {
	return k8s.Remove(ctx, name, namespace, kind, options...)
}

// Remove removes a resource from a given namespace.
// It takes a resource context, the name and namespace of the resource to be removed,
// and optional operation options.
// It returns an error if there was a problem removing the resource.
func Remove(rc global.ResourceContext, name, namespace string, options ...OperationOption) error {
	opts := &operationOptions{}
	for _, opt := range options {
		opt(opts)
	}
	var err error
	var old []k8s.Resource
	if old, err = getExistingManifest(rc.Context(), name, namespace); err != nil {
		return err
	}
	for _, r := range old {

		kind := r.GetObjectKind().GroupVersionKind().Kind
		switch kind {
		case k8s.KindStatefulSet:
			var current = getCurrentRotation(rc.Context(), r.GetName(), rc.Namespace())
			if current != nil {
				if err = doRemove(rc.Context(), fmt.Sprintf("%s---%d", r.GetName(), current.rotation), r.GetNamespace(), k8s.KindStatefulSet, k8s.WithWait(opts.wait)); err != nil {
					rc.Logger().Warnf("failed to remove %s-%s/%s: %s", k8s.KindStatefulSet, rc.Namespace(), r.GetName(), err)
				}
			} else {
				rc.Logger().Warnf(`current rotation not found for %s/%s`, rc.Namespace(), r.GetName())
			}
		default:
			if err = doRemove(rc.Context(), r.GetName(), r.GetNamespace(), r.GetObjectKind().GroupVersionKind().Kind, k8s.WithWait(opts.wait)); err != nil {
				rc.Logger().Warnf("failed to remove %s-%s/%s: %s", r.GetObjectKind().GroupVersionKind().Kind, r.GetNamespace(), r.GetName(), err)
			}
		}
	}
	if err = doRemove(rc.Context(), name+"-manifest", namespace, k8s.KindConfigMap, k8s.WithWait(opts.wait)); err != nil {
		return err
	}
	return nil
}
