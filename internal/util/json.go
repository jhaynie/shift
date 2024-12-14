package util

import "encoding/json"

// JSONStringify will return a stringified json value for val
func JSONStringify(val any) string {
	res, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return string(res)
}
