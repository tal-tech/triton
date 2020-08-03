package consumer

import (
	"bytes"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logger "git.100tal.com/wangxiao_go_lib/xesLogger"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/spf13/cast"
)

var produce sarama.SyncProducer
var once = new(sync.Once)

type KafkaConsumer struct {
	quiter       chan struct{}
	callback     MQCallback
	successCount int64
	errorCount   int64
	wg           *sync.WaitGroup
}

type KafkaConfig struct {
	Host          []string `json:"host"`
	Topic         string   `json:"topic"`
	SASL          *SASL    `json:"sasl"`
	FailTopic     string   `json:"failTopic"`
	FailCount     int      `json:"failCount"`
	ConsumerGroup string   `json:"consumerGroup"`
	ConsumerCount int      `json:"consumerCount"`
	TplMode       int      `json:"tplMode"`
	TplName       string   `json:"tplName"`
	BatchSize     int      `json:"batchSize"`
	BatchTimeout  int      `json:"batchTimeout"`
}

type SASL struct {
	Enabled  bool   `json:"enabled"`
	User     string `json:"user"`
	Password string `json:"password"`
}

func NewKafkaConsumer(configs []*KafkaConfig, callback MQCallback) (consumer *KafkaConsumer, err error) {
	if configs == nil || len(configs) == 0 {
		err = logger.NewError("kafka config not found")
		return
	}
	consumer = new(KafkaConsumer)
	consumer.callback = callback
	consumer.quiter = make(chan struct{}, 0)
	consumer.wg = new(sync.WaitGroup)

	go consumer.count()
	for _, config := range configs {
		for i := 0; i < config.ConsumerCount; i++ {
			consumer.wg.Add(1)
			go consumer.consume(config)
		}
	}
	return
}

func (k *KafkaConsumer) Close() {
	logger.W("IKafaDao", "Close consumer")
	close(k.quiter)
	done := make(chan struct{})
	go func() {
		k.wg.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
		logger.W("IKafaDao", "Close consumer wait for done")
	case <-time.After(time.Second * 2):
		logger.E("IKafaDao", "Close consumer wait timeout")
	}
}

func (k *KafkaConsumer) count() {
	t := time.NewTicker(time.Second * 60)
	for {
		select {
		case <-t.C:
			succ := atomic.SwapInt64(&k.successCount, 0)
			fail := atomic.SwapInt64(&k.errorCount, 0)
			logger.I("KafkaConsumer", "Stat succ:%d,fail:%d", succ, fail)
		}
	}
}

func (k *KafkaConsumer) initSarama(kfg *KafkaConfig) (consumer *cluster.Consumer) {
	cfg := cluster.NewConfig()
	cfg.Config.ClientID = "kafkaWoker"
	if kfg.SASL.Enabled {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = kfg.SASL.User
		cfg.Net.SASL.Password = kfg.SASL.Password
	}

	cfg.Config.Consumer.MaxWaitTime = 500 * time.Millisecond
	cfg.Config.Consumer.MaxProcessingTime = 300 * time.Millisecond
	cfg.Config.Consumer.Offsets.CommitInterval = 350 * time.Millisecond
	cfg.Config.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Config.Consumer.Offsets.Retention = time.Hour * 24 * 15
	cfg.Config.Consumer.Return.Errors = true
	cfg.Group.Return.Notifications = true
	//cfg.Version = sarama.V0_10_2_0
	consumer, err := cluster.NewConsumer(kfg.Host, kfg.ConsumerGroup, strings.Split(kfg.Topic, ","), cfg)
	if err != nil {
		logger.F("Consume", "NewConsumer err:%v", err)
	}
	if kfg.FailTopic != "" {
		once.Do(func() {
			config := sarama.NewConfig()
			config.Producer.Partitioner = sarama.NewHashPartitioner
			config.Producer.RequiredAcks = sarama.WaitForAll
			config.Producer.Return.Successes = true
			if kfg.SASL.Enabled {
				config.Net.SASL.Enable = true
				config.Net.SASL.User = kfg.SASL.User
				config.Net.SASL.Password = kfg.SASL.Password
			}

			if produce, err = sarama.NewSyncProducer(kfg.Host, config); err != nil {
				logger.F("Producer", "NewSyncProducer err:%v", err)
			}
		})
	}
	return
}

func (k *KafkaConsumer) consume(config *KafkaConfig) {
	defer k.wg.Done()
	defer k.recovery()
	logger.I("KafkaConsume", "Start consume from broker %v", config.Host)
	//初始化消费队列
	var consumer *cluster.Consumer
	consumer = k.initSarama(config)
	if consumer != nil {
		defer consumer.Close()
	}
	var addKey bool
	var defaultKey []byte
	if config.TplMode == 1 {
		addKey = true
		defaultKey = []byte(config.Topic + " ")
	}

	go func(c *cluster.Consumer) {
		for notification := range c.Notifications() {
			logger.D("KafkaConsumeNotifaction", "Rebanlance %+v", notification)
		}
	}(consumer)
	go func(c *cluster.Consumer) {
		for err := range c.Errors() {
			logger.E("KafkaConsume", "c.Errors() err:%v", err)
		}
	}(consumer)
	batchMsg := make([]*sarama.ConsumerMessage, 0, config.BatchSize)
	timer := time.NewTimer(time.Millisecond * time.Duration(config.BatchTimeout))
	for {
		message := consumer.Messages()
		select {
		case <-timer.C:
			if config.BatchSize <= 1 {
				continue
			}
			timer.Reset(time.Millisecond * time.Duration(config.BatchTimeout))
			if len(batchMsg) > 0 {
				values := make([][]byte, 0, len(batchMsg))
				for _, event := range batchMsg {
					values = append(values, event.Value)
				}
				value := bytes.Join(values, []byte("\t"))
				if addKey {
					value = append(append([]byte{}, defaultKey...), value...)
				}
				count := 0
				for {
					ret := k.callback(config.TplName, string(config.TplName+"Batch"), value)
					if ret == true {
						atomic.AddInt64(&k.successCount, int64(len(batchMsg)))
						for _, event := range batchMsg {
							logger.D("KafkaConsume", "return_true:KEY:%s,OFFSET:%d,PARTITION:%d", string(event.Key), event.Offset, event.Partition)
							consumer.MarkOffset(event, "")
						}
						break
					} else {
						for _, event := range batchMsg {
							logger.E("KafkaConsume", "return_false:KEY:%s,VAL:%s,OFFSET:%d,PARTITION:%d", string(event.Key), string(event.Value), event.Offset, event.Partition)
						}

						if count >= config.FailCount {
							if config.FailTopic != "" {
								for _, event := range batchMsg {
									for i := 0; i < 3; i++ {
										if err := k.sendByHashPartition(config.FailTopic, event.Value, event.Value); err != nil {
											logger.E("KafkaConsume", "SendToFilerTopic:%s %s %v", config.FailTopic, string(event.Value), err)
											continue
										}
										break
									}
									logger.D("KafkaConsume", "SendToFailTopic :KEY:%s,VAL:%s,OFFSET:%d,PARTITION:%d", string(event.Key), string(event.Value), event.Offset, event.Partition)
									consumer.MarkOffset(event, "")
								}
								break
							} else {
								for _, event := range batchMsg {
									logger.E("KafkaConsume", "Fail Topic is empty:KEY:%s,VAL:%s,OFFSET:%d,PARTITION:%d", string(event.Key), string(event.Value), event.Offset, event.Partition)
								}
								break
							}
						}
						count++
					}
				}
				batchMsg = batchMsg[:0]
			}
		case <-k.quiter:
			logger.W("KafkaConsume", "IKAFKA_RECV_QUIT")
			if err := consumer.CommitOffsets(); err != nil {
				logger.E("KafkaConsume", "IKAFKA_RECV_QUIT Commit Offset Error %v", err)
				time.Sleep(time.Second * 2)
			}
			return
		case event, ok := <-message:
			if !ok {
				continue
			}
			if config.BatchSize > 1 {
				batchMsg = append(batchMsg, event)
				if len(batchMsg) >= config.BatchSize {
					timer.Reset(0)
				}
				continue
			}
			if addKey {
				event.Value = append(append([]byte{}, defaultKey...), event.Value...)
			}
			count := 0
			for {
				ret := k.callback(config.TplName, string(event.Key), event.Value)
				if ret == true {
					atomic.AddInt64(&k.successCount, int64(1))
					logger.D("KafkaConsume", "return_true:KEY:%s,OFFSET:%d,PARTITION:%d", string(event.Key), event.Offset, event.Partition)
					consumer.MarkOffset(event, "")
					break
				} else {
					logger.E("KafkaConsume", "return_false:KEY:%s,VAL:%s,OFFSET:%d,PARTITION:%d", string(event.Key), string(event.Value), event.Offset, event.Partition)

					if count >= config.FailCount {
						if config.FailTopic != "" {
							for i := 0; i < 3; i++ {
								if err := k.sendByHashPartition(config.FailTopic, event.Value, event.Value); err != nil {
									logger.E("KafkaConsume", "SendToFilerTopic:%s %s %v", config.FailTopic, string(event.Value), err)
									continue
								}
								break
							}
							logger.D("KafkaConsume", "Fail Topic is empty:KEY:%s,VAL:%s,OFFSET:%d,PARTITION:%d", string(event.Key), string(event.Value), event.Offset, event.Partition)

							consumer.MarkOffset(event, "")
							break
						} else {
							logger.E("KafkaConsume", "Fail Topic is empty:KEY:%s,VAL:%s,OFFSET:%d,PARTITION:%d", string(event.Key), string(event.Value), event.Offset, event.Partition)
							break
						}
					}
					count++
				}
			}
		}
	}
}

func (k *KafkaConsumer) sendByHashPartition(topic string, data []byte, key []byte) (errr error) {
	msg := &sarama.ProducerMessage{Topic: topic, Key: sarama.ByteEncoder(key), Value: sarama.ByteEncoder(data)}
	if partition, offset, err := produce.SendMessage(msg); err != nil {
		errr = err
		return
	} else {
		logger.D("SendByHashPartition", "[KAFKA_OUT]partition:%d,offset:%d,topic:%s,data:%s", partition, offset, topic, string(data))
	}
	return
}

func (k *KafkaConsumer) recovery() {
	if rec := recover(); rec != nil {
		if err, ok := rec.(error); ok {
			logger.E("IKafkaPanicRecover", "Unhandled error: %v\n stack:%v", err.Error(), cast.ToString(debug.Stack()))
		} else {
			logger.E("IKafkaPanicRecover", "Panic: %v\n stack:%v", rec, cast.ToString(debug.Stack()))
		}
	}
}
