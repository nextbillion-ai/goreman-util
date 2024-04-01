package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/nextbillion-ai/goreman-util/global"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// var dynClient *dynamic.DynamicClient
var clientset *kubernetes.Clientset
var once sync.Once

func Init() error {
	var err error
	once.Do(func() {
		var config *rest.Config
		if config, err = rest.InClusterConfig(); err != nil {
			// If in-cluster config fails, fall back to default kubeconfig path
			kubeconfigPath := os.Getenv("KUBECONFIG")
			if kubeconfigPath == "" {
				kubeconfigPath = clientcmd.NewDefaultClientConfigLoadingRules().GetDefaultFilename()
			}
			// Build config from a kubeconfig filepath
			config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
			if err != nil {
				return
			}
		}
		config.QPS = 100
		config.Burst = 500
		// if dynClient, err = dynamic.NewForConfig(config); err != nil {
		// 	return
		// }
		if clientset, err = kubernetes.NewForConfig(config); err != nil {
			return
		}
	})
	return err
}

func GenManifest(rc global.ResourceContext, chart string, value *global.ManifestValues) (*global.Manifest, error) {
	var err error
	if rc == nil {
		return nil, fmt.Errorf("empty resource context")
	}
	tempFile := rc.TempFile()
	var o []byte
	if o, err = exec.Command("helm", "template", chart, "-f", tempFile).Output(); err != nil {
		return nil, err
	}
	var r global.Manifest
	if err = json.Unmarshal(o, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func GetPvcs(rc global.ResourceContext, stsName string) ([]v1.PersistentVolumeClaim, error) {
	var err error
	if err = Init(); err != nil {
		return nil, err
	}
	var sts *appsv1.StatefulSet
	if sts, err = clientset.AppsV1().StatefulSets(rc.Namespace()).Get(rc.Context(), stsName, metav1.GetOptions{}); err != nil {
		return nil, err
	}
	var pvcs *v1.PersistentVolumeClaimList
	if pvcs, err = clientset.CoreV1().PersistentVolumeClaims(rc.Namespace()).List(rc.Context(), metav1.ListOptions{}); err != nil {
		return nil, err
	}
	var patterns []string
	for _, vct := range sts.Spec.VolumeClaimTemplates {
		patterns = append(patterns, fmt.Sprintf("%s-%s-", vct.ObjectMeta.Name, stsName))
	}
	var result []v1.PersistentVolumeClaim
	for _, pvc := range pvcs.Items {
		for _, pattern := range patterns {
			if strings.Contains(pvc.Name, pattern) {
				result = append(result, pvc)
			}
		}
	}
	return result, nil
}

func Rollout(rc global.ResourceContext, item *global.ManifestItem) error {
	var err error
	if err = Init(); err != nil {
		return err
	}
	switch item.Kind {
	case "Deployment":
		var bin []byte
		if bin, err = json.Marshal(item); err != nil {
			return err
		}
		var deploy appsv1.Deployment
		if err = json.Unmarshal(bin, &deploy); err != nil {
			return err
		}
		deployApi := clientset.AppsV1().Deployments(rc.Namespace())
		if _, err = deployApi.Create(rc.Context(), &deploy, metav1.CreateOptions{}); err != nil {
			if errors.IsAlreadyExists(err) {
				if _, err = deployApi.Update(rc.Context(), &deploy, metav1.UpdateOptions{}); err != nil {
					return err
				}
			} else {
				return err
			}
		}
		if rc.Wait() {
			if err = waitForDeployments(rc, deploy.Name); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported kind: %s", item.Kind)
	}
	return err
}

func waitForDeployments(rc global.ResourceContext, name string) error {
	done := make(chan error)
	go func(name, namespace string, done chan error) {
		defer close(done)
		deployApi := clientset.AppsV1().Deployments(namespace)
		var err error
		var deploy *appsv1.Deployment
		for {
			if deploy, err = deployApi.Get(context.Background(), name, metav1.GetOptions{}); err != nil {
				done <- err
			}
			targetReplicas := int32(1)
			if deploy.Spec.Replicas != nil {
				targetReplicas = *deploy.Spec.Replicas
			}
			if deploy.Status.ObservedGeneration >= deploy.Generation &&
				deploy.Status.ReadyReplicas == targetReplicas &&
				deploy.Status.AvailableReplicas == targetReplicas &&
				deploy.Status.UpdatedReplicas == targetReplicas {
				return
			} else {
				time.Sleep(1 * time.Second)
			}
		}
	}(name, rc.Namespace(), done)
	select {
	case err := <-done:
		return err
	case <-rc.Context().Done():
		return rc.Context().Err()
	}
}
