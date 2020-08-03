package stream

import (
	"context"
	"plugin"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"
)

type SoStream struct {
	sos map[string]func(context.Context, []string) bool
}

func NewSoStream() Stream {
	this := new(SoStream)
	this.sos = make(map[string]func(context.Context, []string) bool)
	return this
}

func (this *SoStream) Operate(ctx context.Context, cmds []string) (ret bool) {
	if len(cmds) < 2 {
		logger.Ex(ctx, "SoOperate", "cmds invalid len<2, cmds:%v", cmds)
		return false
	}
	soname := cmds[1]
	op, ok := this.sos[soname]
	if !ok {
		p, err := plugin.Open("so/" + soname)
		if err != nil {
			logger.Ex(ctx, "SoOperate", "Load so:%s,cmds:%v,err:%v", soname, cmds, err)
			return false
		}
		operate, err := p.Lookup("Operate")
		if err != nil {
			logger.Ex(ctx, "SoOperate", "Lookup so:%s,cmds:%v,err:%v", soname, cmds, err)
			return false
		}
		op, ok = operate.(func(context.Context, []string) bool)
		if !ok {
			logger.Ex(ctx, "SoOperate", "operate assert fail so:%s,cmds:%v", soname, cmds)
			return false
		}
		this.sos[soname] = op
	}
	return op(ctx, cmds[2:])
}
