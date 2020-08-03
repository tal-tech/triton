package tpl

import (
	"context"
	"fmt"
	"os"
	"testing"

	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"
)

func TestTpl(t *testing.T) {
	confutil.USER_CONF_PATH, _ = os.Getwd()
	tpls := NewTpls()
	var task AsyncTask
	task.Cmd = "test"
	task.Ctx = context.Background()
	task.Data = []byte("this-is-tpl-test")
	data, err := tpls.Execute(task.Cmd, &task)
	fmt.Println(data, err)
	//fmt.Println(this.deal(uc.IBroker{}, 0, 0, "", []byte(record), nil))
}
