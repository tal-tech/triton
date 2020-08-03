package tpl

import (
	"bytes"
	"context"
	"io"
	"strings"
	"text/template"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"
	"github.com/Masterminds/sprig"
	"github.com/spf13/cast"
)

type Tpls struct {
	tpls   map[string]*template.Template
	ignore map[string]interface{}
}

func NewTpls() *Tpls {
	this := new(Tpls)
	this.tpls = make(map[string]*template.Template, 0)
	var err error
	tplName := confutil.GetConfDefault("", "tpl", "sql.ini")
	for name, tpl := range this.loadTpl(tplName) {
		if v, ok := tpl.(string); ok {
			this.tpls[name], err = this.compileTpl(name, v)
			if err != nil {
				logger.E("NewTpls", "INVALID_TPL err:%v", err)
				continue
			}
		}
	}
	ignoreName := confutil.GetConf("Ignore", "name")
	this.ignore = this.loadTpl(ignoreName)
	logger.I("TplInit", "TplInitSuccess")
	return this
}

func (this *Tpls) loadTpl(path string) map[string]interface{} {
	ret := make(map[string]interface{}, 0)
	if conf, err := confutil.Load(path); err != nil {
		logger.E("LOAD_TPL", "Load file err:%v", err)
	} else {
		sections := conf.GetSectionList()
		for _, section := range sections {
			fields, err := conf.GetSection(section)
			if err != nil {
				logger.E("LOAD_TPL", "GetSection err:%v", err)
			}
			total := len(fields)
			tpl := ""
			for i := 1; i <= total; i++ {
				tpl = tpl + fields["#"+cast.ToString(i)] + "\n"
			}
			ret[section] = tpl
		}
	}
	return ret
}

func (this *Tpls) compileTpl(name, tpl string) (*template.Template, error) {
	temp := template.New(name).Funcs(GetFuncsMap()).Funcs(sprig.TxtFuncMap())
	return temp.Parse(tpl)
}

type AsyncTask struct {
	Cmd  string
	Ctx  context.Context
	Data []byte
}

func (this *Tpls) Execute(name string, input *AsyncTask) (data []string, err error) {
	if tpl, ok := this.tpls[name]; !ok {
		if _, ok := this.ignore[name]; ok {
			logger.W("SUCCESS_IGNORE", "err:%v cmd=[%s]", err, name)
			return
		}
		return nil, logger.NewError("INVALID_TPL:" + name)
	} else {
		sqls := make([]string, 0)
		var buf bytes.Buffer
		if err = tpl.Execute(&buf, input); err == nil {
			var line string
			for {
				if line, err = buf.ReadString('\n'); err == nil || err == io.EOF {
					tmp := strings.TrimSpace(line)
					if len(tmp) > 0 {
						sqls = append(sqls, tmp)
					}
					if err == io.EOF {
						err = nil
						break
					}
				} else {
					break
				}
			}
		}
		return sqls, err
	}
}
