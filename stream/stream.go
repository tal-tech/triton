package stream

import (
	"context"
)

type StreamManager struct {
	opMap map[BLOCK_TYPE]Stream
}

func NewStreamManager() *StreamManager {
	this := new(StreamManager)
	this.opMap = make(map[BLOCK_TYPE]Stream, 0)
	this.opMap[BLOCK_TYPE_NONE] = NewNoneStream()
	this.opMap[BLOCK_TYPE_CURL] = NewCurlStream()
	this.opMap[BLOCK_TYPE_SQL] = NewSqlStream()
	this.opMap[BLOCK_TYPE_SO] = NewSoStream()
	return this
}

type Stream interface {
	Operate(ctx context.Context, cmds []string) (ret bool)
}

type NoneStream struct {
}

func NewNoneStream() Stream {
	return new(NoneStream)
}

func (this *NoneStream) Operate(ctx context.Context, cmds []string) (ret bool) {
	return true
}

func (this *StreamManager) Operate(ctx context.Context, input []string) (ret bool) {
	blocks := this.block_split(input)
	for _, block := range blocks {
		stm := this.opMap[block.Type]
		ret = stm.Operate(ctx, block.Cmds)
		if !ret {
			return
		}
	}
	return true
}

type BLOCK_TYPE int

const (
	BLOCK_TYPE_NONE BLOCK_TYPE = iota
	BLOCK_TYPE_SQL
	BLOCK_TYPE_CURL
	BLOCK_TYPE_SO
)

type BLOCK struct {
	Type BLOCK_TYPE
	Cmds []string
}

func (this *StreamManager) block_split(input []string) (blocks []BLOCK) {
	blocks = make([]BLOCK, 0)
	types := BLOCK_TYPE_NONE
	cmds := make([]string, 0)
	for _, item := range input {
		if item == "@SQL" {
			if len(cmds) > 0 {
				blocks = append(blocks, BLOCK{Type: types, Cmds: cmds})
				cmds = make([]string, 0)
			}
			types = BLOCK_TYPE_SQL
			cmds = append(cmds, item)
		} else if item == "@CURL" {
			if len(cmds) > 0 {
				blocks = append(blocks, BLOCK{Type: types, Cmds: cmds})
				cmds = make([]string, 0)
			}
			types = BLOCK_TYPE_CURL
			cmds = append(cmds, item)
		} else if item == "@SO" {
			cmds = append(cmds, item)
			if len(cmds) > 0 {
				blocks = append(blocks, BLOCK{Type: types, Cmds: cmds})
				cmds = make([]string, 0)
			}
			types = BLOCK_TYPE_SO
			cmds = append(cmds, item)
		} else if len(item) > 0 {
			cmds = append(cmds, item)
		}
	}
	if len(cmds) > 0 {
		blocks = append(blocks, BLOCK{Type: types, Cmds: cmds})
	}
	return
}
