package stream

import (
	"context"
	"regexp"
	"strings"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
	"git.100tal.com/wangxiao_go_lib/xesTools/httputil"
)

type CurlStream struct {
	regeCache map[string]*regexp.Regexp
}

func NewCurlStream() Stream {
	this := new(CurlStream)
	this.regeCache = make(map[string]*regexp.Regexp, 0)
	return this
}

func (this *CurlStream) Operate(ctx context.Context, cmds []string) (ret bool) {
	var url, method, data, expect string
	var regex *regexp.Regexp
	var header map[string]string
	tt := len(cmds)
	success := false
	for i := 0; i < tt; i++ {
		if cmds[i] == "@CURL" {
			data = ""
			regex = nil
			method, url, header = this.parseCurl(cmds[i+1])
			i = i + 1
		} else if cmds[i] == "@RET" {
			expect = cmds[i+1]
			if strings.HasPrefix(expect, "REG:") {
				expect = expect[4:]
				if reg, ok := this.regeCache[expect]; ok {
					regex = reg
				} else {
					this.regeCache[expect] = regexp.MustCompile(expect)
					regex = this.regeCache[expect]
				}
			}
			i = i + 1
		} else if cmds[i] == "@END" {
			data = strings.TrimSuffix(data, "\n")
			logger.D("CURL_PARAM", "method:%s,url:%s,data:%s,header:%v", method, url, data, header)
			if ret, err := httputil.DoRaw(method, url, data, header); err == nil {
				if regex != nil {
					success = regex.Match(ret)
				}
				logger.I("CURL_RET", "cmds:%v,ret:%s", cmds, string(ret))
			} else {
				logger.E("CURL_RET", "cmds:%s,ret:%s, err:%v", cmds, string(ret), err)
				return false
			}
		} else if cmds[i] == "@IGNORE" {
			success = true
		} else {
			data = data + cmds[i] + "\n"
		}
	}
	return success
}

//POST -H "Content-Type: application/json" -H "Cache-Control: no-cache" http://10.108.230.111/manage/order/_search?q=*
func (this *CurlStream) parseCurl(input string) (method string, url string, header map[string]string) {
	input = strings.Replace(input, ": ", ":", -1)
	arr := strings.Fields(input)
	tt := len(arr)
	method = arr[0]
	url = arr[tt-1]
	header = make(map[string]string)
	for i := 1; i < tt-1; i++ {
		switch arr[i] {
		case "-H":
			head := arr[i+1]
			head = strings.Replace(head, `"`, "", -1)
			head = strings.Replace(head, `'`, "", -1)
			kv := strings.Split(head, ":")
			header[kv[0]] = kv[1]
			i++
		}
	}
	return
}
