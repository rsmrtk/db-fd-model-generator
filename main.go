package main

import (
	"fmt"

	"github.com/rsmrtk/db-fd-model-generator/files"
	"github.com/rsmrtk/db-fd-model-generator/logger"
	"github.com/rsmrtk/db-fd-model-generator/modes"
	"github.com/rsmrtk/db-fd-model-generator/scan"
	"golang.org/x/sync/errgroup"
	"path/filepath"
)

type app struct {
	CreateFMode bool
	Files       *files.Files
	NewTemplate bool
	ModuleName  string
	l           logger.Logger
	*scan.SCAN
}

func NewApp() *app {
	l := logger.New()
	files := files.NewFiles(l)
	mods := modes.FileMode()
	return &app{
		CreateFMode: mods.CreateSqlFilesMode,
		l:           l,
		SCAN:        scan.NewScan(l),
		Files:       files,
		NewTemplate: mods.NewSchemaMode,
	}
}

func main() {

	a := NewApp()

	if err := a.SQLFiles(); err != nil {
		a.l.Fatalln("Error getting file paths:", err)
	}

	moduleName, err := getModuleName()
	if err != nil {
		a.l.Fatalln("Error getting module name:", err)
	}

	paths, err := a.Files.FilePaths()
	if err != nil {
		a.l.Fatalln("Error getting file paths:", err)
	}

	scan, err := a.Scanning(paths, moduleName)
	if err != nil {
		a.l.Println("Error scanning file:", err)
	}

	eg := errgroup.Group{}

	templates := [4]string{
		oldTemplateString + newTemplateString,
		oldTemplateStringMoreThanOnePKAddOn + newTemplateStringMoreThanOnePKAddOn,
		oldTemplateStringMoreThanTwoPKAddOn + newTemplateStringMoreThanTwoPKAddOn,
		oldTemplateStringSecondaryIndexAddOn + newTemplateStringSecondaryIndexAddOn,
	}

	if a.NewTemplate {
		templates = [4]string{
			newTemplateString,
			newTemplateStringMoreThanOnePKAddOn,
			newTemplateStringMoreThanTwoPKAddOn,
			newTemplateStringSecondaryIndexAddOn,
		}
	}

	for _, s := range scan {
		filePath := fmt.Sprintf("%s/%s.go", filepath.Dir(s.Path), s.SD.PackageName)
		template := mainTemplateString

		template += templates[0]
		if s.CountPrimaryKeys > 1 {
			template += templates[1]
			if s.CountPrimaryKeys > 2 {
				template += templates[2]
			}
		}
		if len(s.SD.SecondatyIndexes) > 0 {
			template += templates[3]
		}

		eg.Go(func() error {
			if err := a.Files.ComposeFile(s.SD, filePath, template); err != nil {
				return fmt.Errorf("error composing file: %w", err)
			}
			a.l.Println("File generated: ", filePath)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		a.l.Fatalln("Error waiting for group: ", err)
	}
	if err := a.ServiceFiles(scan, moduleName); err != nil {
		a.l.Fatalln("Error creating service files: ", err)
	}
}

func (a *app) SQLFiles() error {
	if !a.CreateFMode {
		return nil
	}
	err := a.Files.CreateSqlFiles()
	if err != nil {
		return fmt.Errorf("error creating sql files: %w", err)
	}
	return nil
}

func (a *app) ServiceFiles(scanByTable map[string]*scan.ScanData, moduleName string) error {
	if !a.CreateFMode {
		return nil
	}
	builderFile := fmt.Sprintf("%s/sql_builder/builder.go", a.Files.RootDir)
	if err := a.Files.ComposeFile(scanByTable, builderFile, templateQueryBuilderString); err != nil {
		return fmt.Errorf("error composing Builder file: %w", err)
	}
	a.l.Println("Builder file created")
	mf := a.Files.ModelFile(scanByTable, moduleName)
	filePath := fmt.Sprintf("%s/model.go", a.Files.RootDir)
	if err := a.Files.ComposeFile(mf, filePath, templateModelString); err != nil {
		return fmt.Errorf("error composing Model file: %w", err)
	}
	a.l.Println("Model file created")
	optionsFile := fmt.Sprintf("%s/m_options/m_options.go", a.Files.RootDir)
	if err := a.Files.ComposeFile(mf, optionsFile, templateOptionsString); err != nil {
		return fmt.Errorf("error composing Options file: %w", err)
	}
	a.l.Println("Options file created")
	return nil
}
