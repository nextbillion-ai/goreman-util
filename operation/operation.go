package operation

import (
	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/zhchang/goquiver/k8s"
)

func Rollout(rc global.ResourceContext, chartPath string, values map[string]any) error {
	var err error
	var resources []k8s.Resource
	if resources, err = k8s.GenManifest(rc.Context(), chartPath, values); err != nil {
		return err
	}
	for _, r := range resources {
		if err = k8s.Rollout(rc.Context(), r); err != nil {
			return err
		}
	}
	return nil
}

func Remove(rc global.ResourceContext, name, namespace string) error {
	panic("implement me")

}
