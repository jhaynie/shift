package util

import (
	"regexp"
	"strings"
)

var sregex = regexp.MustCompile(`\s{2,}`)

// CleanSQL returns a SQL statement with new lines removed
func CleanSQL(val string) string {
	return sregex.ReplaceAllString(strings.ReplaceAll(val, "\n", " "), " ")
}
