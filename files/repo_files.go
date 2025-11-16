package files

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"text/template"

	"github.com/rsmrtk/db-fd-model-generator/logger"
	"golang.org/x/tools/imports"
)

type Files struct {
	RootDir      string
	SQLFilePaths []string
	l            logger.Logger
}

func NewFiles(lgr logger.Logger) *Files {
	return &Files{
		RootDir: rootDir(lgr),
		l:       lgr,
	}
}

func rootDir(l logger.Logger) string {
	dir, err := os.Getwd()
	if err != nil {
		l.Fatalln("Error getting current directory:", err)
	}
	return dir
}

func (f *Files) FilePaths() ([]string, error) {
	dir := f.RootDir
	if dir == "" {
		return nil, fmt.Errorf("root directory not set")
	}

	// Slice to hold paths of .sql files
	var sqlFiles []string
	// Walk through the directory to find .sql files
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Check if the file has a .sql extension
		// but omit all file in root directory
		if !d.IsDir() && filepath.Ext(path) == ".sql" && filepath.Dir(path) != dir {
			sqlFiles = append(sqlFiles, path)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error walking through directory: %w", err)
	}
	f.SQLFilePaths = sqlFiles
	return sqlFiles, nil
}

func (f *Files) ComposeFile(data any, filePath string, tmlt string) error {
	t, err := template.New("structTemplate").Parse(tmlt)
	if err != nil {
		return fmt.Errorf("failing parsing template %w", err)
	}

	var output bytes.Buffer
	err = t.Execute(&output, data)
	if err != nil {
		return fmt.Errorf("failing executing template %w", err)
	}
	formattedOutput := output.Bytes()
	formattedOutput, err = format.Source(formattedOutput)
	if err != nil {
		f.l.Errorln("Error formatting file: %v, file path: %s", err, filePath)
		formattedOutput = output.Bytes()
	}

	formattedOutput, err = imports.Process("", formattedOutput, nil)
	if err != nil {
		f.l.Errorln("Error formatting imports: %v, file path: %s", err, filePath)
	}

	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return fmt.Errorf("failing creating directory %w", err)
	}

	err = os.WriteFile(filePath, formattedOutput, 0644)
	if err != nil {
		return fmt.Errorf("failing writing file %w", err)
	}
	return nil
}
