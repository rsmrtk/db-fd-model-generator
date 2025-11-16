package scan

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/rsmrtk/db-fd-model-generator/cases"
	"github.com/rsmrtk/db-fd-model-generator/logger"
)

var (
	// Regex patterns
	createTableRegexp    = regexp.MustCompile(`(?i)^CREATE TABLE (\w+)`)
	columnRegexp         = regexp.MustCompile(`(?i)^\s*(\w+)\s+(\w+(?:<[^>]+>)*)(?:\((\d+|MAX)\))?(?:\s+NOT\s+NULL)?`)
	primartKeysRegexp    = regexp.MustCompile(`PRIMARY KEY\s*\(([^)]+)\)`)
	parentTableRegexp    = regexp.MustCompile(`(?i)\bINTERLEAVE IN PARENT\s+(\w+)`)
	secondaryIndexRegexp = regexp.MustCompile(`(?i)^CREATE(?: UNIQUE| SEARCH| UNIQUE NULL_FILTERED)? INDEX (\w+)\s+ON\s+\w+\(([^)]+)\)`)
	// --ENUM(active, inactive, pending, deleted) DEFAULT pending COLUMN status,
	enumRegexp = regexp.MustCompile(`(?i)--ENUM\(([^)]+)\) COLUMN (\w+)`)
)

type Field struct {
	Name  string
	Type  string
	Snake string
}

type PrimaryKey struct {
	Snake      string
	Camel      string
	CamelField string
	Type       string
}

type ChildTable struct {
	Table       string
	PackageName string
	Camel       string
}

type EnumValue struct {
	ConstName string
	Name      string
	Value     string
}

type Enum struct {
	Name       string
	Values     []EnumValue
	ConstNames string
}

type StructTemplateData struct {
	Fields           []Field
	Enums            []Enum
	PackageName      string
	ModuleName       string
	TableName        string
	ProjectName      string
	PrimaryKeys      []PrimaryKey
	ID               string
	Parent           string
	Childs           []ChildTable
	SecondatyIndexes []SecondatyIndex
	// service fields
	l               logger.Logger
	path            string
	primaryKeyCount int
	primaryKeysLine bool
	createTableLine bool
}

type SecondatyIndex struct {
	PrimaryKeys []PrimaryKey
	IndexName   PrimaryKey
	Fields      []PrimaryKey
}

type ScanData struct {
	SD               *StructTemplateData
	Path             string
	CountPrimaryKeys int
}

var spannerTypeMapping = map[string]string{
	"INT64 NOT NULL":     "int64",
	"STRING NOT NULL":    "string",
	"TIMESTAMP NOT NULL": "time.Time",
	"DATE NOT NULL":      "civil.Date",
	"BOOL NOT NULL":      "bool",
	"FLOAT64 NOT NULL":   "float64",
	"NUMERIC NOT NULL":   "big.Rat",
	"INT64":              "spanner.NullInt64",
	"STRING":             "spanner.NullString",
	"BYTES":              "[]byte",
	"BYTES NOT NULL":     "[]byte",
	"TIMESTAMP":          "spanner.NullTime",
	"BOOL":               "spanner.NullBool",
	"FLOAT64":            "spanner.NullFloat64",
	"DATE":               "spanner.NullDate",
	"FLOAT32":            "spanner.NullFloat32",
	"JSON NOT NULL":      "spanner.NullJSON",
	"JSON":               "spanner.NullJSON",
	"NUMERIC":            "spanner.NullNumeric",
	"STRUCT":             "interface{}",
	// "TOKENLIST":          "[]string",
}

var spannerArrTypeMapping = map[string]string{
	"ARRAY":     "[]",
	"INT64":     "int64",
	"STRING":    "string",
	"BYTES":     "[]byte",
	"BOOL":      "bool",
	"FLOAT64":   "float64",
	"TIMESTAMP": "time.Time",
	"DATE":      "civil.Date",
	"NUMERIC":   "big.Rat",
	"JSON":      "spanner.NullJSON",
}

type SCAN struct {
	l logger.Logger
}

func NewScan(l logger.Logger) *SCAN {
	return &SCAN{
		l: l,
	}
}

func (s *SCAN) Scanning(filePaths []string, moduleName string) (map[string]*ScanData, error) {
	sTN := make(map[string]*ScanData)

	for _, path := range filePaths {
		data := StructTemplateData{
			PackageName: strings.ToLower(filepath.Base(filepath.Dir(path))),
			ModuleName:  moduleName,
			ProjectName: filepath.Dir(moduleName),
			l:           s.l,
		}
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("error opening file: %w", err)
		}

		// Read file line by line
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			// Read line
			line := strings.TrimSpace(scanner.Text())

			if err := data.createTableLineCkeck(line); err != nil {
				return nil, err
			}

			if strings.Contains(line, "CONSTRAINT") {
				continue
			}

			data.parentTable(line)

			if data.primaryKeysLine {
				data.secondaryIndex(line)
			} else {

				// Match table name
				if data.tableName(line) {
					continue
				}

				// Match column definitions
				if data.columns(line) {
					continue
				}

				// Match primary keys
				data.primaryKeys(line)
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("error scanning file: %w", err)
		}

		s.l.Printf("Scanned file: %s. Found table: %s", filepath.Base(path), data.TableName)

		sTN[data.TableName] = &ScanData{
			SD:               &data,
			Path:             path,
			CountPrimaryKeys: data.primaryKeyCount,
		}

		file.Close()
	}

	for tableName, sd := range sTN {
		if sd.SD.Parent != "" {
			if _, ok := sTN[sd.SD.Parent]; !ok {
				s.l.Printf("Parent table %s not found\n", sd.SD.Parent)
				continue
			}

			camel, found := strings.CutPrefix(sd.SD.PackageName, sTN[sd.SD.Parent].SD.PackageName+"_")
			if !found {
				camel, _ = strings.CutPrefix(sd.SD.PackageName, "m_")
			}

			camel = cases.ToCamelCase(camel)
			ct := ChildTable{
				Table:       tableName,
				PackageName: sd.SD.PackageName,
				Camel:       camel,
			}
			sTN[sd.SD.Parent].SD.Childs = append(sTN[sd.SD.Parent].SD.Childs, ct)
		}

	}
	return sTN, nil
}

func parceArray(s string) string {
	s = strings.Replace(s, "<", " ", -1)
	s = strings.Replace(s, ">", " ", -1)
	if strings.Contains(s, "NOT NULL") {
		s = strings.ReplaceAll(s, "NOT NULL", "")
	}
	arr := strings.Fields(s)

	var goType string

	for _, a := range arr {
		for i, t := range a {
			if t == '(' {
				a = a[:i]
				break
			}
		}
		t, ok := spannerArrTypeMapping[strings.ToUpper(a)]
		if !ok {
			goType = "interface{}" // Default to interface{} if unknown type
		} else {
			goType += t
		}
	}
	return goType
}

func (st *StructTemplateData) createTableLineCkeck(line string) error {
	if strings.Contains(line, "CREATE TABLE") {
		if st.createTableLine {
			return fmt.Errorf("%s. Multiple tables found", st.path)
		}
		st.createTableLine = true
	}
	return nil
}

func (st *StructTemplateData) parentTable(line string) {
	parentTableLine := parentTableRegexp.FindStringSubmatch(line)
	if len(parentTableLine) > 0 {
		st.Parent = strings.TrimSpace(parentTableLine[1])
	}
}

func (st *StructTemplateData) tableName(line string) bool {
	if matches := createTableRegexp.FindStringSubmatch(line); matches != nil {
		st.TableName = cases.ToSnakeCase(matches[1])
		return true
	}
	return false
}

func (st *StructTemplateData) columns(line string) bool {
	if matches := columnRegexp.FindStringSubmatch(line); matches != nil {
		if strings.Contains(matches[1], "allow_commit_timestamp") {
			return true
		}

		if strings.Contains(matches[2], "TOKENLIST") {
			return true
		}

		var goType string
		columnName := matches[1]
		sqlType := matches[2]
		if strings.Contains(matches[0], "NOT NULL") {
			sqlType += " NOT NULL"
		}

		if strings.Contains(sqlType, "ARRAY") {
			goType = parceArray(sqlType)
		} else {
			t, ok := spannerTypeMapping[strings.ToUpper(sqlType)]
			if !ok {
				goType = "interface{}" // Default to interface{} if unknown type
			} else {
				goType = t
			}
		}

		if !strings.Contains(matches[1], "INTERLEAVE") {
			st.Fields = append(st.Fields, Field{
				Name:  cases.ToCamelCase(columnName),
				Type:  goType,
				Snake: columnName,
			})
		}
	}
	if matches := enumRegexp.FindStringSubmatch(line); matches != nil {
		enumList := strings.Split(matches[1], ",")
		columnName := matches[2]
		getName := func(s string) string {
			name := s
			if len(name) == 0 {
				name = "unspecified"
			}
			return cases.ToCamelCase(fmt.Sprintf("enum_%s_%s", columnName, name))
		}
		enum := &Enum{
			Name:   cases.ToCamelCase("enum_" + columnName),
			Values: make([]EnumValue, len(enumList)),
		}
		for i, e := range enumList {
			enumValue := strings.TrimSpace(strings.Replace(e, "'", "", -1))
			name := getName(enumValue)
			enum.Values[i] = EnumValue{
				ConstName: enum.Name,
				Name:      name,
				Value:     enumValue,
			}
			if i == 0 {
				enum.ConstNames = name
			} else {
				enum.ConstNames = fmt.Sprintf("%s, %s", enum.ConstNames, name)
			}
		}
		st.Enums = append(st.Enums, *enum)
	}
	return false
}

func (st *StructTemplateData) primaryKeys(line string) {
	primaryKeysLine := primartKeysRegexp.FindStringSubmatch(line)
	if len(primaryKeysLine) > 0 {
		st.primaryKeysLine = true
		pk := strings.Split(primaryKeysLine[1], ",")
		st.primaryKeyCount = len(pk)
		primaryKeys := make([]PrimaryKey, len(pk))

		st.ID = strings.TrimSpace(pk[len(pk)-1])
		for i, key := range pk {
			primaryKeys[i] = st.composePrimaryKey(key)
		}
		st.PrimaryKeys = primaryKeys
	}
}

func (st *StructTemplateData) composePrimaryKey(key string) PrimaryKey {
	var primaryKey PrimaryKey
	key = strings.TrimSpace(key)
	if strings.Contains(key, " ") {
		key = strings.Split(key, " ")[0]
	}
	primaryKey.Snake = key
	key = cases.ToCamelCase(key)
	primaryKey.CamelField = key
	key = cases.FirstLetterToLower(key)
	primaryKey.Camel = key

	for _, f := range st.Fields {
		if f.Snake == primaryKey.Snake {
			primaryKey.Type = f.Type
			break
		}
	}
	return primaryKey
}

func (st *StructTemplateData) secondaryIndex(line string) {
	secondaryIndexLine := secondaryIndexRegexp.FindStringSubmatch(line)
	if len(secondaryIndexLine) > 0 {
		st.l.Printf("Secondary index found: %s", secondaryIndexLine[1])
		var fields []PrimaryKey
		for _, field := range strings.Split(secondaryIndexLine[2], ",") {
			fields = append(fields, st.composePrimaryKey(field))
		}
		sidx := SecondatyIndex{
			IndexName:   st.composePrimaryKey(secondaryIndexLine[1]),
			Fields:      fields,
			PrimaryKeys: st.PrimaryKeys,
		}
		st.SecondatyIndexes = append(st.SecondatyIndexes, sidx)
	}
}
