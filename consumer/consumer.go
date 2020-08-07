package consumer

import (
	"encoding/json"
	"io/ioutil"
	"sync"

	logger "github.com/tal-tech/loggerX"
	"github.com/tal-tech/xtools/confutil"
)

type ConsumerManager struct {
	cfg      *Configure
	wg       *sync.WaitGroup
	kafka    *KafkaConsumer
	rabbitmq *RabbitmqConsumer
	rocketmq *RocketmqConsumer
	nsq      *NSQConsumer
}

type MQ struct {
	Kafka    bool `json:"kafka"`
	Rabbitmq bool `json:"rabbitmq"`
	Rocketmq bool `json:"rocketmq"`
	NSQ      bool `json:"nsq"`
}

type Configure struct {
	Enabled  *MQ               `json:"enabled"`
	Kafka    []*KafkaConfig    `json:"kafka"`
	Rabbitmq []*RabbitmqConfig `json:"rabbitmq"`
	Rocketmq []*RocketmqConfig `json:"rocketmq"`
	Nsq      []*NSQConfig      `json:"nsq"`
}

type MQCallback func(TplName string, Key string, Value []byte) bool

func NewConsumerManager(callback MQCallback) *ConsumerManager {
	var err error
	manager := new(ConsumerManager)
	cfg := gLoadConfigure()
	manager.cfg = cfg
	if cfg.Enabled.Kafka {
		manager.kafka, err = NewKafkaConsumer(cfg.Kafka, callback)
		if err != nil {
			logger.E("NewConsumerManager", "NewKafkaConsumer err:%v", err)
		}
	}
	if cfg.Enabled.Rabbitmq {
		manager.rabbitmq, err = NewRabbitmqConsumer(cfg.Rabbitmq, callback)
		if err != nil {
			logger.E("NewConsumerManager", "NewRabbitConsumer err:%v", err)
		}
	}

	if cfg.Enabled.Rocketmq {
		manager.rocketmq, err = NewRocketmqConsumer(cfg.Rocketmq, callback)
		if err != nil {
			logger.E("NewConsumerManager", "NewRocketConsumer err:%v", err)
		}
	}

	if cfg.Enabled.NSQ {
		manager.nsq, err = NewNSQConsumer(cfg.Nsq, callback)
		if err != nil {
			logger.E("NewConsumerManager", "NewNSQConsumer err:%v", err)
		}
	}

	return manager
}

func (c *ConsumerManager) Close() {
	if c.cfg.Enabled.Kafka {
		c.kafka.Close()
	}
	if c.cfg.Enabled.Rabbitmq {
		c.rabbitmq.Close()
	}
	if c.cfg.Enabled.Rocketmq {
		c.rocketmq.Close()
	}
	if c.cfg.Enabled.NSQ {
		c.nsq.Close()
	}
	return
}

//Load Config.json
func gLoadConfigure() *Configure {
	b, err := ioutil.ReadFile(confutil.GetConf("DEFAULT", "mqConfigPath"))
	if err != nil {
		logger.F("Consumer", "Read config file err:%v", err)
	}
	config := new(Configure)
	err = json.Unmarshal(b, config)
	if err != nil {
		logger.F("Consumer", "json Unmarshal err:%v", err)
	}
	return config
}
