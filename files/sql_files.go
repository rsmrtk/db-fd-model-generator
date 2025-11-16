package files

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/rsmrtk/db-fd-model-generator/cases"
)

type TableData struct {
	TableDDL        string
	TableName       string
	PackageName     string
	ParentTableName string
	Path            string
}

func (f *Files) CreateSqlFiles() error {
	dir := f.RootDir
	if dir == "" {
		return fmt.Errorf("root directory not set")
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		f.l.Fatalln("Error reading directory:", err)
	}

	var path string

	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".sql" {
			if path != "" {
				f.l.Fatalln("Multiple sql files found in root directory")
			}
			path = file.Name()
		}
	}

	if path == "" {
		return fmt.Errorf("no sql file found in root directory")
	}

	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}

	var tableDatas []*TableData

	createTableRegex := regexp.MustCompile(`(?i)^CREATE TABLE (\w+)`)

	scanner := bufio.NewScanner(file)

	var table string

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.ReplaceAll(line, "`", "")

		if matches := createTableRegex.FindStringSubmatch(line); matches != nil {
			if line != "" {
				tableData := TableData{
					TableDDL: table,
				}
				tableDatas = append(tableDatas, &tableData)
			}
			table = line + "\n"
		} else {
			table += line + "\n"
		}

	}
	tableDatas = append(tableDatas, &TableData{TableDDL: table})

	parentTableRegex := regexp.MustCompile(`(?i)\bINTERLEAVE IN PARENT\s+(\w+)`)

	for _, td := range tableDatas {
		scanner := bufio.NewScanner(strings.NewReader(td.TableDDL))
		for scanner.Scan() {
			line := scanner.Text()

			parentTableLine := parentTableRegex.FindStringSubmatch(line)
			if len(parentTableLine) > 0 {
				td.ParentTableName = strings.TrimSpace(parentTableLine[1])
			}
			tableName := createTableRegex.FindStringSubmatch(line)
			if len(tableName) > 0 {
				td.TableName = tableName[1]
				words := strings.Split(td.TableName, "_")
				lastWord := words[len(words)-1]
				lastWordToSingular := cases.PluralToSingular(lastWord)
				words[len(words)-1] = lastWordToSingular
				td.PackageName = "m_" + strings.Join(words, "_")
			}

		}
	}

	for _, td := range tableDatas {
		findPaths(tableDatas, td, dir)
	}

	// create the sql files
	for _, td := range tableDatas {
		if td.PackageName == "" {
			fmt.Printf("Skipping %+v\n", td)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(td.Path), os.ModePerm); err != nil {
			return fmt.Errorf("error creating directory: %w", err)
		}
		file, err := os.Create(td.Path)
		if err != nil {
			return fmt.Errorf("error creating file: %w", err)
		}
		defer file.Close()
		if _, err = file.WriteString(td.TableDDL); err != nil {
			return fmt.Errorf("error writing to file: %w", err)
		}
		f.l.Printf("Created file: %s\n", td.Path)
	}

	return nil
}

func findPaths(tableDatas []*TableData, td *TableData, basePath string) string {
	if td.Path != "" {
		return td.Path // If path is already set, return it
	}

	// If the table has a parent, find the parent's path recursively
	if td.ParentTableName != "" {
		for _, parent := range tableDatas {
			if parent.TableName == td.ParentTableName {
				parentPath := findPaths(tableDatas, parent, basePath)
				td.Path = filepath.Join(filepath.Dir(parentPath), td.PackageName, td.PackageName+".sql")
				return td.Path
			}
		}
	}

	// If no parent, set the path at the base level
	td.Path = filepath.Join(basePath, td.PackageName, td.PackageName+".sql")
	return td.Path
}
