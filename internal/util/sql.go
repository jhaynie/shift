package util

import (
	"regexp"
	"strings"
)

var sregex = regexp.MustCompile(`\s{2,}`)

// CleanSQL returns a SQL statement with new lines removed
func CleanSQL(val string) string {
	return strings.TrimSpace(sregex.ReplaceAllString(strings.ReplaceAll(val, "\n", " "), " "))
}

// IsFunctionCall returns true if the val looks like a function call
func IsFunctionCall(val string) bool {
	return val[0:1] != "'" && strings.Contains(val, "(") && strings.Contains(val, ")")
}
