package main

import (
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"triton/ctrl"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
	"git.100tal.com/wangxiao_go_lib/xesLogger/builders"
	"git.100tal.com/wangxiao_go_lib/xesTools/confutil"
	"git.100tal.com/wangxiao_go_lib/xesTools/limitutil"
	"git.100tal.com/wangxiao_go_lib/xesTools/pprofutil"

	"github.com/Shopify/sarama"
	"github.com/spf13/cast"
)

var (
	loggerXml = "conf/log.xml"
	stop      = make(chan struct{})
)

func main() {
	//init conf
	confutil.InitConfig()

	logger.InitLogger(loggerXml)
	builder := new(builders.TraceBuilder)
	builder.SetTraceDepartment("XueYan")
	builder.SetTraceVersion("0.1")
	logger.SetBuilder(builder)
	if err := limitutil.GrowToMaxFdLimit(); err != nil {
		logger.E("Fd Error", "try grow to max limit under normal priviledge, failed")
		return
	}

	defer recovery()
	go pprofutil.Pprof()
	go dealSignal()

	sarama.Logger = &saramaLog{}
	exec()

	defer logger.Close()
}

func dealSignal() {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		stop <- struct{}{}
	}()
}

func recovery() {
	if rec := recover(); rec != nil {
		if err, ok := rec.(error); ok {
			logger.E("PanicRecover", "Unhandled error: %v\n stack:%v", err.Error(), cast.ToString(debug.Stack()))
		} else {
			logger.E("PanicRecover", "Panic: %v\n stack:%v", rec, cast.ToString(debug.Stack()))
		}
	}
}
func exec() {
	defer recovery()
	ctrl := ctrl.NewCtrl(stop)

	for {
		select {
		case <-stop:
			logger.W("Exec", "Process Stop...")
			ctrl.Close()
			logger.W("Exec", "Process Stop Complete")
			return
		}
	}
}
