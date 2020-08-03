# Triton 

## Background
-----
Triton is a high performance, high stability plug-in type messaging middleware consumer written in pure Go language,which supports the mainstream message queues in the market, such as Kafka, RabbitMQ, RocketMQ, NSQ, etc.And it is easy to be extended to meet different business requirements in production environment.

## Framework
------
The framework of triton is shown as below.

![image](https://github.com/hhtlxhhxy/triton/blob/master/img/frame.jpg)

## Quickstart

### kafka
-----

#### 1. Start zookeeper
```shell
./bin/zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
```
#### 2. Start kafka
```shell
./bin/kafka-server-start /usr/local/etc/kafka/server.properties
```
#### 3. Create topic
```shell
./bin/kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
```
#### 4. Modify config for kafka in triton

```json
{
  "enabled": {
    "kafka": true
  },
  "kafka": [
    {
      "consumerGroup": "test0",
      "consumerCount": 1,
      "host": [
        "127.0.0.1:9092"
      ],
      "sasl": {
        "enabled": false,
        "user": "",
        "password": ""
      },
      "topic": "test",
      "failTopic": "xes_exercise_fail",
      "tplMode": 1,
      "tplName": "test"
    }
  ]
}
```
* tpl config
```shell
[test]
-={{$ctx := .Ctx}}{{$arg := .Data}}{{printf "%v\n" $arg}}
-=@NONE
```

#### 5. Make
```shell
make
```
#### 6. Run
```shell
./bin/triton -c ../conf/conf.ini
```
