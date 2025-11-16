package cases

import (
	"log"
	"strings"
	"unicode"

	"github.com/jinzhu/inflection"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func FirstLetterToLower(s string) string {

	if len(s) == 0 {
		return s
	}

	r := []rune(s)
	r[0] = unicode.ToLower(r[0])

	return string(r)
}

func ToCamelCase(input string) string {
	parts := strings.Split(input, "_")
	for i := range parts {
		parts[i] = cases.Title(language.English).String(parts[i])
	}
	res := strings.Join(parts, "")
	// check out if the last two characters are 'Id'
	if strings.HasSuffix(res, "Id") && len(res) > 2 {
		res = strings.ReplaceAll(res, "Id", "ID")
	}
	return res
}

func ToSnakeCase(input string) string {
	var result []byte
	for i, c := range input {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				result = append(result, '_')
			}
			c += 'a' - 'A'
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func PluralToSingular(word string) string {

	if strings.Contains(word, "media") {
		log.Printf("word: %s\n", word)
		return word
	}
	// Handle common pluralization rules
	return inflection.Singular(word)
}

func CheckToReservedWord(word string) (bool, string) {
	if _, ok := reservedSQLKeywords[word]; ok {
		word = "`" + word + "`"
		return true, word
	}
	return false, word
}

var reservedSQLKeywords = map[string]struct{}{
	"select":      {},
	"from":        {},
	"where":       {},
	"insert":      {},
	"update":      {},
	"delete":      {},
	"join":        {},
	"inner":       {},
	"outer":       {},
	"left":        {},
	"right":       {},
	"full":        {},
	"group":       {},
	"order":       {},
	"by":          {},
	"having":      {},
	"distinct":    {},
	"limit":       {},
	"offset":      {},
	"as":          {},
	"on":          {},
	"in":          {},
	"and":         {},
	"or":          {},
	"not":         {},
	"null":        {},
	"is":          {},
	"like":        {},
	"between":     {},
	"case":        {},
	"when":        {},
	"then":        {},
	"else":        {},
	"end":         {},
	"exists":      {},
	"all":         {},
	"any":         {},
	"union":       {},
	"intersect":   {},
	"except":      {},
	"create":      {},
	"alter":       {},
	"drop":        {},
	"table":       {},
	"view":        {},
	"index":       {},
	"primary":     {},
	"key":         {},
	"foreign":     {},
	"references":  {},
	"constraint":  {},
	"default":     {},
	"check":       {},
	"trigger":     {},
	"values":      {},
	"set":         {},
	"explain":     {},
	"with":        {},
	"rollback":    {},
	"commit":      {},
	"transaction": {},
}
