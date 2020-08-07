package ctrl

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/tal-tech/xtools/confutil"
)

func TestCtrl(t *testing.T) {
	confutil.USER_CONF_PATH, _ = os.Getwd()
	this := NewCtrl(nil)
	fmt.Println(this.deal("test", "key", []byte("demo?ctx=context&key=value this-is-a-test")))
	time.Sleep(time.Second)
	//fmt.Println(this.deal(uc.IBroker{}, 0, 0, "", []byte(record), nil))
}

func TestParseTask(t *testing.T) {
	confutil.USER_CONF_PATH, _ = os.Getwd()
	this := NewCtrl(nil)
	data := []byte("test?ctx=context&key=value this is a test")
	task := this.taskParse(data, 26)
	fmt.Printf("cmd:%s,ctx:%v,data:%s", task.Cmd, task.Ctx, task.Data)
}
