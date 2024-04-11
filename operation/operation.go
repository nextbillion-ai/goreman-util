package operation

import (
	"context"
	"regexp"
	"time"

	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/zhchang/goquiver/k8s"
	"github.com/zhchang/goquiver/raw"
)

type manifest struct {
	resource k8s.Resource
	changed  bool
}

type toRemove struct {
	name      string
	namespace string
	kind      k8s.Kind
}

func getManifests(ctx context.Context, chartPath string, values raw.Map) (old []k8s.Resource, new []k8s.Resource, newMap map[string]*manifest, err error) {
	if new, err = k8s.GenManifest(ctx, chartPath, values); err != nil {
		return
	}
	var name, namespace string
	if name, err = raw.ChainGet[string](values, "global", "name"); err != nil {
		return
	}
	if namespace, err = raw.ChainGet[string](values, "global", "namespace"); err != nil {
		return
	}
	var _t []k8s.Resource
	var cfg *k8s.ConfigMap
	if cfg, err = k8s.Get[*k8s.ConfigMap](ctx, name+"-manifest", namespace); err != nil {
		return
	}
	if _t, err = k8s.DecodeAllYAML(cfg.Data["manifest"]); err == nil {
		old = _t
	}
	newMap = map[string]*manifest{}
	for _, r := range new {
		newMap[r.GetObjectKind().GroupVersionKind().Kind+"-"+r.GetName()] = &manifest{resource: r}
	}
	err = nil
	return
}

var blackLists = []*regexp.Regexp{regexp.MustCompile(`^(docker.io\/)*redis`), regexp.MustCompile(`^(docker.io\/)*postgres`)}

func shouldRename(sts *k8s.StatefulSet) bool {
	var rotationFlag = sts.ObjectMeta.Annotations["foreman/rotation"]
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

func shouldRotate(df raw.Map, sts *k8s.StatefulSet) bool {
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
		return true
	}
	for _, key := range specChanges {
		if _, ok := rotatekeys[key]; !ok {
			return true
		}
	}
	var templateMap raw.Map
	if templateMap, err = raw.ChainGet[raw.Map](df, "spec", "template"); err == nil {
		if _, ok := templateMap["labels"]; ok {
			return true
		}
	}
	return false
}

func Rollout(rc global.ResourceContext, chartPath string, values raw.Map) error {
	var err error
	var old, new []k8s.Resource
	var newMap map[string]*manifest
	if old, new, newMap, err = getManifests(rc.Context(), chartPath, values); err != nil {
		return err
	}

	var toRemoves []toRemove

	for _, r := range old {
		kind := r.GetObjectKind().GroupVersionKind().Kind
		key := kind + "-" + r.GetName()
		var ok bool
		var nm *manifest
		if nm, ok = newMap[key]; !ok {
			toRemoves = append(toRemoves, toRemove{name: r.GetName(), namespace: r.GetNamespace(), kind: kind})
			continue
		}

		if r.GetObjectKind().GroupVersionKind().Kind == k8s.KindStatefulSet {
			var df raw.Map
			if df, err = raw.Diff(r, nm.resource); err != nil {
				return err
			}
			//var changed = len(df) > 0
			var sts *k8s.StatefulSet
			if sts, err = k8s.Parse[*k8s.StatefulSet](r); err != nil {
				return err
			}
			shouldRotate(df, sts)
			//try rotation

		}
	}

	for _, r := range new {
		if err = k8s.Rollout(rc.Context(), r); err != nil {
			return err
		}
	}
	for _, r := range toRemoves {
		_ = k8s.Remove(rc.Context(), r.name, r.namespace, r.kind, k8s.WithWait(2*time.Minute))
	}
	return nil
}

func Remove(rc global.ResourceContext, name, namespace string) error {
	panic("implement me")

}
