package ctrl

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"triton/consumer"
	"triton/stream"
	"triton/tpl"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
)

type Ctrl struct {
	tpls      *tpl.Tpls
	consumers *consumer.ConsumerManager
	streams   *stream.StreamManager
}

func NewCtrl(stop chan struct{}) *Ctrl {
	this := new(Ctrl)
	this.tpls = tpl.NewTpls()
	this.streams = stream.NewStreamManager()
	this.consumers = consumer.NewConsumerManager(this.deal)
	return this
}

func (this *Ctrl) Close() {
	this.consumers.Close()
}

func (this *Ctrl) taskParse(value []byte, pos int) *tpl.AsyncTask {
	task := new(tpl.AsyncTask)
	task.Ctx = context.Background()
	task.Data = value[pos+1:]
	path := string(value[0:pos])
	s := strings.Split(path, "?")
	if len(s) > 0 {
		task.Cmd = s[0]
	}
	if len(s) > 1 {
		urls, e := url.ParseQuery(s[1])
		if e != nil {
			logger.W("TaskParse", "parse url error,url:%s,err:%v", s[1], e)
		} else {
			for k, v := range urls {
				task.Ctx = context.WithValue(task.Ctx, k, v[0])
			}
		}
	}
	return task
}

func (this *Ctrl) deal(TplName string, Key string, Value []byte) (ret bool) {
	pos := bytes.Index(Value, []byte(" "))
	pos_tab := bytes.Index(Value, []byte("\t"))
	if (pos <= 0 || pos_tab < pos) && pos_tab > 0 {
		pos = pos_tab
	}
	if pos > 0 {
		task := this.taskParse(Value, pos)
		cmd := task.Cmd
		if TplName != "" {
			cmd = TplName
		}
		if data, err := this.tpls.Execute(cmd, task); err == nil {
			//插件方式
			ret = this.streams.Operate(task.Ctx, data)
		} else {
			ret = false
			logger.E("deal", "TPL Execute err:%v,key:%s,value:%s", err, Key, Value)
		}
	} else {
		info := fmt.Sprintf("k:%s,v:%s", Key, string(Value))
		logger.E("INVALID_FORMAT", info)
	}
	if !ret {
		time.Sleep(time.Millisecond * 500)
	}
	logger.D("deal", "ret:%t,key:%s,value:%s", ret, Key, Value)
	return ret
}
