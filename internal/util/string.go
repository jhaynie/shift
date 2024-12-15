package util

// Plural returns the singular or plural depending on the count
func Plural(count int, singular string, plural string) string {
	if count == 0 || count > 1 {
		return plural
	}
	return singular
}
