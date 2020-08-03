/**
 *Created by He Haitao at 2019/11/7 3:50 下午
 */
package consumer

import (
	"io/ioutil"
	"log"
	"runtime/debug"
	"sync"
	"time"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"

	"github.com/nsqio/go-nsq"
	"github.com/spf13/cast"
)

type NSQConsumer struct {
	quiter   chan struct{}
	callback MQCallback
	wg       *sync.WaitGroup
}

type NSQConfig struct {
	NSQLookup     []string `json:"nsqLookup"`
	Topic         string   `json:"topic"`
	Channel       string   `json:"channel"`
	ConsumerCount int      `json:"consumerCount"`
	TplMode       int      `json:"tplMode"`
	TplName       string   `json:"tplName"`
}

func NewNSQConsumer(configs []*NSQConfig, callback MQCallback) (consumer *NSQConsumer, err error) {
	if configs == nil || len(configs) == 0 {
		err = logger.NewError("nsq config not found")
		return
	}
	consumer = new(NSQConsumer)
	consumer.callback = callback
	consumer.quiter = make(chan struct{}, 0)
	consumer.wg = new(sync.WaitGroup)

	for _, config := range configs {
		go consumer.run(config)
	}
	return
}

func (r *NSQConsumer) Close() {
	logger.W("NSQConsumer", "Close consumer")
	close(r.quiter)
	r.wg.Wait()
}

type ConsumerHandler struct {
	q              *nsq.Consumer
	callback       MQCallback
	config         *NSQConfig
	consumerCount  int
	messagesGood   int
	messagesFailed int
}

type tbLog interface {
	Log(...interface{})
}

func (h *ConsumerHandler) LogFailedMessage(message *nsq.Message) {
	h.messagesFailed++
	h.q.Stop()
}

func (h *ConsumerHandler) HandleMessage(message *nsq.Message) error {
	if h.config.TplMode == 1 {
		message.Body = append(append([]byte{}, []byte(h.config.Topic+" ")...), message.Body...)
	}

	for {
		ret := h.callback(h.config.TplName, h.config.Topic, message.Body)
		if ret == true {
			logger.D("NSQConsumer", "consumer:%d,return_true:TOPIC:%s", h.consumerCount, h.config.Topic)
			break
		} else {
			logger.E("NSQConsumer", "consumer:%d,return_false:TOPIC:%s,VAL:%s", h.consumerCount, h.config.Topic, message.Body)
			//TODO:retry and send fail topic
			break
		}
	}
	return nil
}

func (r *NSQConsumer) consumer(kfg *NSQConfig, count int) {
	defer r.wg.Done()

	config := nsq.NewConfig()
	config.DefaultRequeueDelay = 0
	config.MaxBackoffDuration = time.Millisecond * 50

	c, err := nsq.NewConsumer(kfg.Topic, kfg.Channel, config)
	if err != nil {
		logger.E("NSQConsumer", " new nsq consumer error %v", err)
		return
	}
	handle := &ConsumerHandler{
		q:             c,
		config:        kfg,
		callback:      r.callback,
		consumerCount: count,
	}
	c.SetLogger(log.New(ioutil.Discard, "", log.LstdFlags), nsq.LogLevelError)
	c.AddHandler(handle)
	err = c.ConnectToNSQLookupds(kfg.NSQLookup)
	if err != nil {
		logger.E("NSQConsumer", " connect to nsqlookup error %v", err)
		return
	}
	for {
		select {
		case <-r.quiter:
			logger.W("NSQConsumer", " Receive Quit")
			c.Stop()
			return
		}
	}
}

func (r *NSQConsumer) run(config *NSQConfig) {
	defer r.recovery()

	for i := 0; i < config.ConsumerCount; i++ {
		r.wg.Add(1)
		go r.consumer(config, i)
	}
}

func (r *NSQConsumer) recovery() {
	if rec := recover(); rec != nil {
		if err, ok := rec.(error); ok {
			logger.E("NSQConsumer PanicRecover", "Unhandled error: %v\n stack:%v", err.Error(), cast.ToString(debug.Stack()))
		} else {
			logger.E("NSQConsumer PanicRecover", "Panic: %v\n stack:%v", rec, cast.ToString(debug.Stack()))
		}
	}
}
