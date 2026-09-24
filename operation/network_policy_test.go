package operation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zhchang/goquiver/k8s"
)

func TestApplyResourceDispatchesNetworkPolicy(t *testing.T) {
	resource, err := k8s.DecodeYAML("apiVersion: networking.k8s.io/v1\nkind: NetworkPolicy\nmetadata:\n  name: test\n  namespace: agent\n")
	require.NoError(t, err)

	original := applyNetworkPolicyResource
	t.Cleanup(func() { applyNetworkPolicyResource = original })
	called := false
	applyNetworkPolicyResource = func(_ context.Context, got k8s.Resource) error {
		called = true
		require.Equal(t, "test", got.GetName())
		return nil
	}

	require.NoError(t, applyResource(context.Background(), resource))
	require.True(t, called)
}

func TestRemoveDispatchesNetworkPolicy(t *testing.T) {
	original := removeNetworkPolicyResource
	t.Cleanup(func() { removeNetworkPolicyResource = original })
	called := false
	removeNetworkPolicyResource = func(_ context.Context, name, namespace string) error {
		called = true
		require.Equal(t, "test", name)
		require.Equal(t, "agent", namespace)
		return nil
	}

	require.NoError(t, doRemove(context.Background(), "test", "agent", kindNetworkPolicy))
	require.True(t, called)
}
