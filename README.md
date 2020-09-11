## <img src="https://github.com/hhtlxhhxy/triton/blob/master/img/triton.jpg" alt="image-20200803155136931" style="zoom:50%;" />

## Background
-----
Triton is a high performance, high stability plug-in type messaging middleware consumer written in pure Go language,which supports the mainstream message queues in the market, such as Kafka, RabbitMQ, RocketMQ, NSQ, etc.And it is easy to be extended to meet different business requirements in production environment.

## Document
-----
[Document](https://tal-tech.github.io/triton-doc/)

## Framework
------
The framework of triton is shown as below.

<img src="https://github.com/hhtlxhhxy/triton/blob/master/img/frame.jpg" alt="image-20200803155136931" style="zoom:50%;" />

## Quickstart

### Consumer messages from kafka by triton.
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

#### 5. Run
```shell
tar -zxvf triton.tar.gz
cd triton/
make
./bin/triton -c ../conf/conf.ini
```
