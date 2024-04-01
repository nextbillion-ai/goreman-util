package ctx

import (
	"github.com/nextbillion-ai/goreman-util/asset"
	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/nextbillion-ai/goreman-util/resource"
)

type Ctx struct {
	ID                string
	Assets            map[string]*asset.Asset
	TempFiles         []string
	Cluster           string
	BasePath          string
	GlobalSpecPlugins []global.Plugin
	WorkingDir        string
	Dry               bool
	Namespace         string
	Name              string
	Resource          *resource.Resource
}
