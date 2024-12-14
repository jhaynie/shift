package util

import "encoding/json"

// JSONStringify will return a stringified json value for val
func JSONStringify(val any, pretty ...bool) string {
	var res []byte
	var err error
	if len(pretty) > 0 {
		res, err = json.MarshalIndent(val, " ", "  ")
	} else {
		res, err = json.Marshal(val)
	}
	if err != nil {
		panic(err)
	}
	return string(res)
}
