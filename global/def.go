package global

import (
	"context"
	"encoding/json"
)

type Spec struct {
	Asset struct {
		Typ     string `json:"type"`
		Release string `json:"release"`
	} `json:"asset"`
	App json.RawMessage `json:"app"`
}

type AssetContext interface {
	WorkPath() string
	BasePath() string
}

type ResourceContext interface {
	AssetContext
	Cluster() string
	Context() context.Context
	TempFile() string
	SetNamespace(value string)
	Namespace() string
	Cleanup()
	Wait() bool
}

type ManifestValues struct {
	App    json.RawMessage `json:"app"`
	Global json.RawMessage `json:"global"`
}

type ManifestItem struct {
	Kind       string          `json:"kind"`
	ApiVersion string          `json:"apiVersion"`
	Metadata   json.RawMessage `json:"metadata"`
	Spec       json.RawMessage `json:"spec"`
}

type Manifest []*ManifestItem
