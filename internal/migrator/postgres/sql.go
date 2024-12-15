package postgres

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"

	"github.com/jhaynie/shift/internal/util"
	"github.com/lib/pq"
)

// Taken from https://github.com/shopmonkeyus/eds
// MIT License from https://github.com/shopmonkeyus/eds/blob/main/LICENSE
// https://github.com/shopmonkeyus/eds/blob/main/internal/drivers/postgresql/sql.go

const magicEscape = "$_P_$"

var safeCharacters = regexp.MustCompile(`^["/.,;:$%/@!#$%^&*(){}\[\]|\\<>?~a-zA-Z0-9_\- ]+$`)

var badCharacters = regexp.MustCompile(`\x00`) // in v1 we have the null character that show up in messages

func quoteString(str string) string {
	if len(str) != 0 && badCharacters.MatchString(str) {
		str = badCharacters.ReplaceAllString(str, "")
	}
	if len(str) == 0 || safeCharacters.MatchString(str) {
		return `'` + str + `'`
	}
	return magicEscape + str + magicEscape
}

func quoteBytes(buf []byte) string {
	return `'\x` + hex.EncodeToString(buf) + "'"
}

func quoteValue(arg any) (str string) {
	switch arg := arg.(type) {
	case nil:
		str = "null"
	case int:
		str = strconv.FormatInt(int64(arg), 10)
	case int8:
		str = strconv.FormatInt(int64(arg), 10)
	case int16:
		str = strconv.FormatInt(int64(arg), 10)
	case int32:
		str = strconv.FormatInt(int64(arg), 10)
	case *int32:
		if arg == nil {
			str = "null"
		} else {
			str = strconv.FormatInt(int64(*arg), 10)
		}
	case int64:
		str = strconv.FormatInt(arg, 10)
	case *int64:
		if arg == nil {
			str = "null"
		} else {
			str = strconv.FormatInt(*arg, 10)
		}
	case float32:
		str = strconv.FormatFloat(float64(arg), 'f', -1, 32)
	case float64:
		str = strconv.FormatFloat(arg, 'f', -1, 64)
	case *float64:
		if arg == nil {
			str = "null"
		} else {
			str = strconv.FormatFloat(*arg, 'f', -1, 64)
		}
	case bool:
		str = strconv.FormatBool(arg)
	case *bool:
		if arg == nil {
			str = "null"
		} else {
			str = strconv.FormatBool(*arg)
		}
	case []byte:
		str = quoteBytes(arg)
	case *string:
		if arg == nil {
			str = "null"
		} else {
			str = quoteString(*arg)
		}
	case string:
		str = quoteString(arg)
	case *time.Time:
		if arg == nil {
			str = "null"
		} else {
			str = (*arg).Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
		}
	case time.Time:
		str = arg.Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999Z07:00:00'")
	case []string:
		var ns []string
		for _, thes := range arg {
			ns = append(ns, pq.QuoteLiteral(thes))
		}
		str = quoteString(util.JSONStringify(ns))
	case []interface{}:
		str = quoteString(util.JSONStringify(arg))
	default:
		value := reflect.ValueOf(arg)
		if value.Kind() == reflect.Ptr {
			if value.IsNil() {
				str = "null"
			} else {
				if value.Elem().Kind() == reflect.Struct {
					str = quoteString(util.JSONStringify(arg))
				} else {
					str = quoteString(fmt.Sprintf("%v", value.Elem().Interface()))
				}
			}
		} else {
			str = quoteString(util.JSONStringify(arg))
		}
	}
	return str
}

var needsQuote = regexp.MustCompile(`[A-Z0-9_\s]`)
var keywords = regexp.MustCompile(`(?i)\b(USER|SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|GROUP BY|ORDER BY|HAVING|AND|OR|CREATE|DROP|ALTER|TABLE|INDEX|ON|INTO|VALUES|SET|AS|DISTINCT|TYPE|DEFAULT|ORDER|GROUP|LIMIT|SUM|TOTAL|START|END|BEGIN|COMMIT|ROLLBACK|PRIMARY|AUTHORIZATION|BINARY)\b`)

func quoteIdentifier(val string) string {
	if needsQuote.MatchString(val) || keywords.MatchString(val) {
		return pq.QuoteIdentifier(val)
	}
	return val
}
