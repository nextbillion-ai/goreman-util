// Package asset provides utilities for managing and manipulating assets.
// It includes functionalities for JSON schema compilation, caching, and
// manipulation of JSON objects. It also provides utilities for handling
// concurrent operations on these assets.
//
// The package uses the "github.com/santhosh-tekuri/jsonschema/v5" library
// for JSON schema compilation and validation. It uses the "github.com/zhchang/goquiver/safe"
// library for safe concurrent operations.
//
// The package initializes a JSON schema compiler in the Draft4 mode and
// provides a function to remove empty required fields from a JSON object.
package asset

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nextbillion-ai/goreman-util/global"
	"github.com/nextbillion-ai/gsg/lib/object"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"github.com/zhchang/goquiver/safe"
)

var loaderCache = safe.NewMap[string, *sync.Once]()
var compiler *jsonschema.Compiler

func init() {
	compiler = jsonschema.NewCompiler()
	compiler.Draft = jsonschema.Draft4
}

func removeEmptyRequired(obj map[string]interface{}) {
	for key, value := range obj {
		switch v := value.(type) {
		case []interface{}:
			// If the value is an array, process each element if it's an object
			for _, item := range v {
				if itemObj, ok := item.(map[string]interface{}); ok {
					removeEmptyRequired(itemObj)
				}
			}
		case map[string]interface{}:
			// If the value is an object, process it recursively
			removeEmptyRequired(v)
		}

		// Remove the "required" key if it's an empty array
		if key == "required" {
			if arr, ok := value.([]interface{}); ok && len(arr) == 0 {
				delete(obj, key)
			}
		}
	}
}

// Asset represents an asset in the system. It contains information about the asset's
// ID, type, release, local path, URL, and associated JSON schema.
//
// Fields:
// id: A unique identifier for the asset.
// typ: The type of the asset.
// release: The release version of the asset.
// localPath: The local file path where the asset is stored.
// url: The URL where the asset can be accessed.
// schema: The JSON schema associated with the asset, used for validation.
type Asset struct {
	id        string
	typ       string
	release   string
	localPath string
	url       string
	schema    *jsonschema.Schema
}

// New creates a new Asset instance with the specified asset context, type, and release.
// It returns a pointer to the created Asset and an error, if any.
// The asset context (rc) must not be nil.
// The type (typ) and release must be non-empty strings.
// The function initializes the local path, URL, and ID of the asset.
// It also sets up a loader for the asset and performs necessary operations to populate the asset's local directory.
// Finally, it compiles the asset's schema using the specified schema file.
func New(rc global.AssetContext, typ, release string) (*Asset, error) {
	var err error
	if rc == nil {
		return nil, fmt.Errorf("RunContext is nil")
	}
	a := &Asset{
		typ:     typ,
		release: release,
	}
	wp := rc.WorkPath()
	if wp == "" {
		wp = "/tmp/.operator/cache/assets"
	}
	a.localPath = fmt.Sprintf("%s/%s/%s", wp, typ, release)
	a.url = fmt.Sprintf("%s/assets/%s/releases/%s", global.MustHaveOptions().Basepath, typ, release)
	a.id = a.typ + "-" + a.release
	var once sync.Once
	loaderCache.SetIfNotExists(a.id, &once)
	loader, _ := loaderCache.Get(a.id)
	if loader == nil {
		return nil, fmt.Errorf("[unexpected error] can't get asset once loader")
	}
	if loader.Do(func() {
		if _, err = exec.Command("mkdir", "-p", a.localPath).Output(); err != nil {
			return
		}
		var dir *object.Object
		if dir, err = object.New(a.url); err != nil {
			return
		}
		var ors []*object.ObjectResult
		if ors, err = dir.List(false); err != nil {
			return
		}
		for _, or := range ors {
			var o *object.Object
			if o, err = object.New(or.Url); err != nil {
				return
			}
			filePath := a.localPath + "/" + strings.TrimLeft(strings.ReplaceAll(or.Url, a.url, ""), "/")
			var fp *os.File
			if fp, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
				return
			}
			if err = o.Read(fp); err != nil {
				return
			}
		}
		var absPath string
		if absPath, err = filepath.Abs(a.localPath + "/schema.json"); err != nil {
			return
		}
		var data []byte
		if data, err = os.ReadFile(absPath); err != nil {
			return
		}
		schemaValue := map[string]any{}
		if err = json.Unmarshal(data, &schemaValue); err != nil {
			return
		}
		removeEmptyRequired(schemaValue)
		if data, err = json.Marshal(schemaValue); err != nil {
			return
		}
		if err = os.WriteFile(absPath, data, 0644); err != nil {
			return
		}
		if a.schema, err = compiler.Compile("file://" + filepath.ToSlash(absPath)); err != nil {
			return
		}
	}); err != nil {
		return nil, err
	}

	return a, nil
}

// Validate validates the given values against the asset's schema.
// It returns an error if the schema is not initialized or if the validation fails.
func (a *Asset) Validate(values map[string]any) error {
	if a.schema == nil {
		return fmt.Errorf("schema not initaizlied")
	}
	return a.schema.Validate(values)
}

// ChartPath returns the path to the chart.tgz file for the asset.
func (a *Asset) ChartPath() string {
	return a.localPath + "/chart.tgz"
}

/*



  async plugin (context) {
    const ppath = `${this.basePath}/plugin.js`
    if (await pathExists(ppath)) {
      return (await import(ppath)).Impl
    }
    return Default
  }

*/
