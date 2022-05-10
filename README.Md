# Kafclient
> move by teng231/kafclient
helper for create system using kafka to streaming and events driven base.

[![Go](https://github.com/teng231/kafclient/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/teng231/kafclient/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/princjef/gomarkdoc.svg)](https://pkg.go.dev/github.com/teng231/kafclient)

### Install

``` bash
 go get github.com/teng231/kafclient
```
have 3 env variables for config:
* KAFKA_NUM_PARTITION: config number of partition( default: 3)
* KAFKA_REPLICATION_FACTOR: config replication factor.(default:  -1)
* KAFKA_VERSION: for set kafka version for sarama config

you can overide 3 variables for cluster configs

### Usage

Library have 2 modules publisher and consumer, or use can use both.
For example to setup:

* Both setup

``` go
func NewKafClient() error {
	ps := &psub.KafClient{}
	brokers := strings.Split("localhost:9002,localhost:9003", ",")
	ps.InitPublisher(brokers...)
	err := ps.InitConsumerGroup(cf.KafClientSubscription, brokers...)
	if err != nil {
		return err
	}
	return nil
}

```


* Publisher setup


``` go

func NewKafClient() error {
	ps := &psub.KafClient{}
	brokers := strings.Split("localhost:9002,localhost:9003", ",")
	ps.InitPublisher(brokers...)
	if err != nil {
		return err
	}
	return nil
}


// using for publish event
func SendMessage(msg string) {
    sms := map[string]interface{}{
		Body: "hello everyone",
        App:  "company.vn",
		To:   []string{"0374140511"},
	}
    if err := ps.Publish("topic_send_example", sms); err != nil {
		log.Print(err)
	}
}


```


* Consumer setup
``` go

func main() {

    // if this task running will block main thread
    onStreamRunning(...)

}


func NewKafClient() error {
	ps := &psub.KafClient{}
	brokers := strings.Split("localhost:9002,localhost:9003", ",")
	err := ps.InitConsumerGroup(cf.KafClientSubscription, brokers...)
	if err != nil {
		return err
	}
	return nil
}

// using for publish event
func SendMessage() error {
    sms := map[string]interface{}{
		Body: "hello everyone",
        App:  "company.vn",
		To:   []string{"0374140511"},
	}
    if err := ps.Publish("topic_send_example", sms); err != nil {
		log.Print(err)
        return err
	}
    return nil
}


func SendMessageExtraConfig() error {
    sms := map[string]interface{}{
		Body: "hello everyone",
        App:  "company.vn",
		To:   []string{"0374140511"},
	}
    if err := ps.PublishWithConfig(Topic{Name:"topic_send_example", AutoCommit: false}, &SenderConfig{
		Header: map[string]string{"sender_by": "containerX"},
		Metadata: "any_if_you_want",
	}, sms); err != nil {
		log.Print(err)
        return err
	}
    return nil
}

// send message help sync task
func SendMessageToSinglePartition() error {
    sms := map[string]interface{}{
		Body: "hello everyone",
        App:  "company.vn",
		To:   []string{"0374140511"},
	}
    if err := ps.PublishWithConfig(Topic{Name:"topic_send_example", AutoCommit: false, Partition: ToPInt32(1)}, &SenderConfig{
		Header: map[string]string{"sender_by": "containerX"},
		Metadata: "any_if_you_want",
	}, sms); err != nil {
		log.Print(err)
        return err
	}
    return nil
}


func onListenUserInsert(payload []byte, h *Account, fnCommit func()) {
	defer fnCommit()
	user := &User{}
    onCreateUser(user)
    ...
}


type MsgHandler struct {
	f     func([]byte, *Account, func())
	topic *KafClient.Topic
}

var (
	backend_user_insert_user = "backend_user_insert_user"
    numberOfConcurrents = 1
	subsList                   = map[string]*MsgHandler{
		backend_user_insert_user: {onListenUserInsert, &KafClient.Topic{
			Name:       backend_user_insert_user,
			AutoCommit: false,
		}},
	}
)

func onStreamRunning(ps *KafClient.KafClient, h *Account) {
	log.Print("Stream start")
	msgBuf := make(chan KafClient.Message, 1000)

	numberOfConcurrents := 1
	topics := make([]*KafClient.Topic, 0, 100)

	for _, msghandler := range kafkaTopics {
		topics = append(topics, msghandler.topic)
	}

	go func() {
		for {
			psMessage := <-msgBuf
			msghandler := subsList[psMessage.Topic]
			if msghandler.f != nil {
				msghandler.f(psMessage.Body, h, psMessage.Commit)
			}
		}
	}()
	err := ps.OnAsyncSubscribe(topics, numberOfConcurrents, msgBuf)
	if err != nil {
		log.Print(err)
	}
}
```
