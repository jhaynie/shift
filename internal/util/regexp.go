package util

import "regexp"

var IsNumber = regexp.MustCompile(`^-?\d+(.\d+)?$`)
var IsInteger = regexp.MustCompile(`^-?\d+$`)
var IsFloat = regexp.MustCompile(`^-?\d+(.\d+)?$`)
