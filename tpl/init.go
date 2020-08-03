package tpl

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"

	"github.com/spf13/cast"
)

var funcsMap = template.FuncMap{}

func init() {
	funcsMap["json_encode"] = func(v interface{}) (string, error) {
		r, e := json.Marshal(v)
		return string(r), e
	}
	funcsMap["json_decode"] = func(v interface{}) (interface{}, error) {
		var o interface{}
		e := json.Unmarshal([]byte(cast.ToString(v)), &o)
		return o, e
	}
	funcsMap["printf"] = func(format string, a ...interface{}) (n int, err error) {
		return fmt.Printf(format, string(a[0].([]byte)))
	}
	funcsMap["println"] = func(a ...interface{}) (n int, err error) {
		return fmt.Println(a)
	}
	funcsMap["echo"] = func(v interface{}) (ret interface{}, err error) {
		return "OK", nil
	}
	funcsMap["split"] = func(sep string, v interface{}) []string {
		return strings.Split(cast.ToString(v), sep)
	}
	funcsMap["split_message"] = func(sep string, v interface{}) []string {
		return strings.Split(cast.ToString(v), sep)
	}
	funcsMap["substr"] = func(in string, start, end int) string {
		return in[start:end]
	}
	funcsMap["string"] = func(v interface{}) (ret string, err error) {
		return cast.ToString(v), nil
	}
	funcsMap["join"] = func(sep string, v ...interface{}) string {
		total := len(v)
		ret := make([]string, total)
		for i := 0; i < total; i++ {
			ret[i] = cast.ToString(v[i])
		}
		return strings.Join(ret, sep)
	}
	funcsMap["add"] = func(a, b interface{}) string {
		return cast.ToString(cast.ToInt(a) + cast.ToInt(b))
	}
	funcsMap["md5"] = func(v ...string) (ret string, err error) {
		h := md5.New()
		for _, i := range v {
			_, err = h.Write([]byte(i))
			if err != nil {
				return
			}
		}
		ret = hex.EncodeToString(h.Sum(nil))
		return
	}
	funcsMap["self"] = func(v interface{}) (ret string, err error) {
		vStr := cast.ToString(v)
		if strings.Contains(vStr, "kafka") {
			fmt.Println("kafka exist")
		} else {
			fmt.Println("kafka not exist")
			return "FAIL`", errors.New("fail")
		}
		return "OK", nil
	}
}

func GetFuncsMap() template.FuncMap {
	return funcsMap
}
