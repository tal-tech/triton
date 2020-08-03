/**
 *Created by He Haitao at 2019/11/4 5:59 下午
 */

package consumer

import (
	"context"
	"runtime/debug"
	"sync"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"

	"github.com/apache/rocketmq-client-go"
	"github.com/apache/rocketmq-client-go/consumer"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/spf13/cast"
)

type RocketmqConsumer struct {
	quiter   chan struct{}
	callback MQCallback
	wg       *sync.WaitGroup
}

const (
	PushMode = iota
	PullMode
)

type RocketmqConfig struct {
	ConsumerMode  int                       `json:"consumerMode"` //0:push 1:pull
	ConsumerGroup string                    `json:"consumerGroup"`
	ConsumerCount int                       `json:"consumerCount"`
	NameServer    []string                  `json:"nameServer"`
	Topic         string                    `json:"topic"`
	Tags          string                    `json:"tags"`
	Retry         int                       `json:"retry"`
	Offset        consumer.ConsumeFromWhere `json:"offset"`
	TplMode       int                       `json:"tplMode"`
	TplName       string                    `json:"tplName"`
}

func NewRocketmqConsumer(configs []*RocketmqConfig, callback MQCallback) (consumer *RocketmqConsumer, err error) {
	if configs == nil || len(configs) == 0 {
		err = logger.NewError("rocketmq config not found")
		return
	}
	consumer = new(RocketmqConsumer)
	consumer.callback = callback
	consumer.quiter = make(chan struct{}, 0)
	consumer.wg = new(sync.WaitGroup)

	for _, config := range configs {
		go consumer.run(config)
	}
	return
}

func (r *RocketmqConsumer) Close() {
	logger.W("RocketmqConsumer", "Close consumer")
	close(r.quiter)
	r.wg.Wait()
}

func (r *RocketmqConsumer) pushConsumer(kfg *RocketmqConfig) {
	defer r.wg.Done()

	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(kfg.ConsumerGroup),
		consumer.WithNameServer(kfg.NameServer),
		consumer.WithRetry(kfg.Retry),
		consumer.WithConsumeFromWhere(kfg.Offset),
	)
	if err != nil {
		logger.E("RocketmqPushConsumer", " new push consumer error %v", err)
		return
	}

	ms := new(consumer.MessageSelector)
	ms.Type = consumer.TAG
	ms.Expression = kfg.Tags

	err = c.Subscribe(kfg.Topic, *ms, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {

		for _, me := range msgs {
			if kfg.TplMode == 1 {
				me.Body = append(append([]byte{}, []byte(me.GetTags()+" ")...), me.Body...)
			}

			for {
				ret := r.callback(kfg.TplName, me.Topic, me.Body)
				if ret == true {
					logger.D("RocketmqConsumer", "return_true:TOPIC:%s, Tags:%s", me.Topic, me.GetTags())
					break
				} else {
					logger.E("RocketmqConsumer", "return_false:TOPIC:%s,Tags:%s,VAL:%s", me.Topic, me.GetTags(), string(me.Body))
					//TODO:retry and send fail topic
					break
				}
			}
		}

		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		logger.E("RocketmqPushConsumer", " push consumer subscribe error %v", err)
		return
	}
	err = c.Start()
	if err != nil {
		logger.E("RocketmqPushConsumer", " push consumer start error %v", err)
		return
	}
	for {
		select {
		case <-r.quiter:
			logger.W("RocketmqPushConsumer", " Receive Quit")
			return
		}
	}
}

func (r *RocketmqConsumer) pullConsumer(kfg *RocketmqConfig) {
	//TODO:support pull consumer
}

func (r *RocketmqConsumer) run(config *RocketmqConfig) {
	defer r.recovery()

	for i := 0; i < config.ConsumerCount; i++ {
		r.wg.Add(1)
		go r.consume(config, i)
	}
}

func (r *RocketmqConsumer) consume(config *RocketmqConfig, count int) {
	if config.ConsumerMode == PushMode {
		go r.pushConsumer(config)
	}
	if config.ConsumerMode == PullMode {
		go r.pullConsumer(config)
	}
}

func (r *RocketmqConsumer) recovery() {
	if rec := recover(); rec != nil {
		if err, ok := rec.(error); ok {
			logger.E("RocketmqConsumer PanicRecover", "Unhandled error: %v\n stack:%v", err.Error(), cast.ToString(debug.Stack()))
		} else {
			logger.E("RocketmqConsumer PanicRecover", "Panic: %v\n stack:%v", rec, cast.ToString(debug.Stack()))
		}
	}
}
