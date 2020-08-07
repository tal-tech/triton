/**
 *Created by He Haitao at 2019/10/30 5:23 下午
 */
package consumer

import (
	"errors"
	"runtime/debug"
	"sync"
	"time"

	logger "github.com/tal-tech/loggerX"

	"github.com/spf13/cast"
	"github.com/streadway/amqp"
)

type RabbitmqConsumer struct {
	quiter   chan struct{}
	callback MQCallback
	wg       *sync.WaitGroup
	configs  []*RabbitmqConfig
}

type RabbitmqConfig struct {
	URL           string `json:"url"`
	FailCount     int    `json:"failCount"`
	FailExchange  string `json:"failExchange"`
	IsReject      bool   `json:"isReject"`
	PrefetchCount int    `json:"prefetchCount"`
	ConsumerQueue string `json:"consumerQueue"`
	ConsumerCount int    `json:"consumerCount"`
	TplMode       int    `json:"tplMode"`
	TplName       string `json:"tplName"`
}

func NewRabbitmqConsumer(configs []*RabbitmqConfig, callback MQCallback) (consumer *RabbitmqConsumer, err error) {
	if configs == nil || len(configs) == 0 {
		err = logger.NewError("rabbitmq config not found")
		return
	}
	consumer = new(RabbitmqConsumer)
	consumer.callback = callback
	consumer.quiter = make(chan struct{}, 0)
	consumer.wg = new(sync.WaitGroup)
	consumer.configs = configs
	for _, config := range configs {
		err := consumer.run(config)
		if err != nil {
			return consumer, err
		}
	}
	return
}

func (r *RabbitmqConsumer) Close() {
	logger.W("RabbitmqConsumer", "Close consumer")
	close(r.quiter)
	r.wg.Wait()
}

func (r *RabbitmqConsumer) initConnect(kfg *RabbitmqConfig) (*amqp.Connection, error) {
	connect, err := amqp.Dial(kfg.URL)
	if err != nil {
		logger.E("RabbitmqConsumer", " init connect error %v", err)
		return connect, err
	}
	return connect, nil
}

func (r *RabbitmqConsumer) initChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	channel := new(amqp.Channel)
	var err error
	if conn != nil {
		channel, err = conn.Channel()
		if err != nil {
			logger.E("RabbitmqConsumer", " init channel error %v", err)
			return channel, err
		}
		return channel, nil
	}
	err = errors.New("connect is nil")
	logger.E("RabbitmqConsumer", " init channel error %v", err)
	return channel, err
}

func (r *RabbitmqConsumer) initConsumer(kfg *RabbitmqConfig, channel *amqp.Channel) (<-chan amqp.Delivery, error) {
	var err error
	if channel != nil {
		msgs, err := channel.Consume(
			kfg.ConsumerQueue,
			"",
			false,
			false, // exclusive
			false, // no local
			false, // no wait
			nil,   // arg
		)
		if err != nil {
			logger.E("RabbitmqConsumer", "init queue bind error %v", err)
			return msgs, err
		}
		return msgs, nil
	}
	err = errors.New("channel is nil")
	logger.E("RabbitmqConsumer", "init queue bind error %v", err)
	return nil, err
}

func (r *RabbitmqConsumer) run(config *RabbitmqConfig) error {
	defer r.recovery()

	connect, err := r.initConnect(config)
	if err != nil {
		logger.E("RabbitmqConsumer", " init connect error %v", err)
		return err
	}

	channel, err := r.initChannel(connect)
	if err != nil {
		logger.E("RabbitmqConsumer", " init channel error %v", err)
		return err
	}

	err = channel.Qos(config.PrefetchCount, 0, false)
	if err != nil {
		logger.E("RabbitmqConsumer", " init channel qos error %v", err)
		return err
	}

	for i := 0; i < config.ConsumerCount; i++ {
		r.wg.Add(1)
		go r.consume(config, connect, channel, i)
	}

	return nil
}

func (r *RabbitmqConsumer) recov(config *RabbitmqConfig, conn *amqp.Connection) {
	if conn.IsClosed() {
		for {
			err := r.run(config)
			if err != nil {
				logger.E("RabbitmqConsumer recov", " faild %v", err)
				time.Sleep(1 * time.Second)
			} else {
				break
			}
		}
	}
}

func (r *RabbitmqConsumer) consume(config *RabbitmqConfig, conn *amqp.Connection, channel *amqp.Channel, count int) {
	defer conn.Close()
	if count == 0 {
		defer r.recov(config, conn)
	}
	defer r.wg.Done()

	msgs, err := r.initConsumer(config, channel)
	if err != nil {
		logger.E("RabbitmqConsumer", " init consumer error %v", err)
		return
	}
	for {
		select {
		case <-r.quiter:
			logger.W("RabbitmqConsumer", " RabbitmqConsumer_RECV_QUIT")
			return
		case event, ok := <-msgs:
			if !ok {
				return
			}
			if config.TplMode == 1 {
				event.Body = append(append([]byte{}, []byte(event.RoutingKey+" ")...), event.Body...)
			}
			count := 0

			for {
				ret := r.callback(config.TplName, event.RoutingKey, event.Body)
				if ret == true {
					logger.D("RabbitmqConsumer", "return_true:KEY:%s, consumer count:%d", event.RoutingKey, count)
					err := event.Ack(false)
					if err != nil {
						logger.E("RabbitmqConsumer", "ACK Fail:KEY:%s,VAL:%v", event.RoutingKey, event.Body)
						err := event.Ack(false) //retry
						if err != nil {
							logger.E("RabbitmqConsumer", "ACK Retry Fail:KEY:%s,VAL:%v", event.RoutingKey, event.Body)
						}
					}
					break
				} else {
					logger.E("RabbitmqConsumer", "return_false:KEY:%s,VAL:%v", event.RoutingKey, event.Body)

					if count >= config.FailCount {
						if config.FailExchange != "" {
							for i := 0; i < 3; i++ {
								if err := channel.Publish(
									config.FailExchange,
									event.RoutingKey,
									false,
									false,
									amqp.Publishing{
										ContentType: "text/plain",
										Body:        event.Body,
									},
								); err != nil {
									logger.E("RabbitmqConsumer", "SendToFailExchange:%s %s %v", config.FailExchange, string(event.Body), err)
									continue
								}
								break
							}
							logger.D("RabbitmqConsumer", "SendToFailExchange:%s KEY:%s,VAL:%v", config.FailExchange, event.RoutingKey, event.Body)

							err := event.Ack(false)
							if err != nil {
								logger.E("RabbitmqConsumer", "ACK Fail:KEY:%s,VAL:%v", event.RoutingKey, event.Body)
								err := event.Ack(false) //retry
								if err != nil {
									logger.E("RabbitmqConsumer", "ACK Retry Fail:KEY:%s,VAL:%v", event.RoutingKey, event.Body)
								}
							}
							break
						} else {
							logger.W("RabbitmqConsumer", "FailTopic is empty:KEY:%s,VAL:%v", event.RoutingKey, event.Body)
							event.Reject(config.IsReject)
							break
						}
					}
					count++
				}
			}
		}
	}
}

func (r *RabbitmqConsumer) recovery() {
	if rec := recover(); rec != nil {
		if err, ok := rec.(error); ok {
			logger.E("RabbitmqConsumer PanicRecover", "Unhandled error: %v\n stack:%v", err.Error(), cast.ToString(debug.Stack()))
		} else {
			logger.E("RabbitmqConsumer PanicRecover", "Panic: %v\n stack:%v", rec, cast.ToString(debug.Stack()))
		}
	}
}
