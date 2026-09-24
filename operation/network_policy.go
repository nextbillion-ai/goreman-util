package operation

import (
	"context"
	"os"

	"github.com/zhchang/goquiver/k8s"
	networkingv1 "k8s.io/api/networking/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typednetworkingv1 "k8s.io/client-go/kubernetes/typed/networking/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const kindNetworkPolicy k8s.Kind = "NetworkPolicy"

var networkPolicyAPI = func(namespace string) (typednetworkingv1.NetworkPolicyInterface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfigPath := os.Getenv("KUBECONFIG")
		if kubeconfigPath == "" {
			kubeconfigPath = clientcmd.NewDefaultClientConfigLoadingRules().GetDefaultFilename()
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, err
		}
	}
	config.QPS = 100
	config.Burst = 500
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return client.NetworkingV1().NetworkPolicies(namespace), nil
}

var applyNetworkPolicyResource = func(ctx context.Context, resource k8s.Resource) error {
	desired, err := k8s.Parse[*networkingv1.NetworkPolicy](resource)
	if err != nil {
		return err
	}
	api, err := networkPolicyAPI(desired.Namespace)
	if err != nil {
		return err
	}
	live, err := api.Get(ctx, desired.Name, metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		_, err = api.Create(ctx, desired, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	desired.ResourceVersion = live.ResourceVersion
	_, err = api.Update(ctx, desired, metav1.UpdateOptions{})
	return err
}

var removeNetworkPolicyResource = func(ctx context.Context, name, namespace string) error {
	api, err := networkPolicyAPI(namespace)
	if err != nil {
		return err
	}
	err = api.Delete(ctx, name, metav1.DeleteOptions{})
	if k8serrors.IsNotFound(err) {
		return nil
	}
	return err
}
