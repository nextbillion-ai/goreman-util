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

type Asset struct {
	id        string
	typ       string
	release   string
	localPath string
	url       string
	schema    *jsonschema.Schema
}

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
	a.url = fmt.Sprintf("%s/assets/%s/releases/%s", rc.BasePath(), typ, release)
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
		if a.schema, err = compiler.Compile("file://" + filepath.ToSlash(absPath)); err != nil {
			return
		}
	}); err != nil {
		return nil, err
	}

	return a, nil
}

func (a *Asset) Validate(values json.RawMessage) error {
	if a.schema == nil {
		return fmt.Errorf("schema not initaizlied")
	}
	return a.schema.Validate(values)
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
