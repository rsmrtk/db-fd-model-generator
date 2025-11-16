package files

import (
	"path/filepath"
	"strings"

	"github.com/rsmrtk/db-fd-model-generator/cases"
	"github.com/rsmrtk/db-fd-model-generator/scan"
)

type ModelFile struct {
	Tables      []ModelTable
	ModuleName  string
	PackageName string
	ProjectName string
}

type ModelTable struct {
	TableName   string
	PackageName string
}

func (f *Files) ModelFile(scanByTable map[string]*scan.ScanData, moduleName string) *ModelFile {
	var modelFile ModelFile
	for _, s := range scanByTable {

		if s.SD.Parent == "" {
			mt := ModelTable{
				TableName:   cases.PluralToSingular(cases.ToCamelCase(s.SD.TableName)),
				PackageName: s.SD.PackageName,
			}
			modelFile.Tables = append(modelFile.Tables, mt)
		}
	}

	modelFile.PackageName = filepath.Base(moduleName)
	modelFile.PackageName = strings.ReplaceAll(modelFile.PackageName, "-", "_")
	modelFile.ProjectName = filepath.Dir(moduleName)
	modelFile.ModuleName = moduleName

	return &modelFile
}
