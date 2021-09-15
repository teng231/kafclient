# Psub

helper for create system using kafka to streaming and events driven base.

### Install

``` bash
 go get github.com/teng231/psub
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
func NewPubSub() error {
	ps := &psub.PubSub{}
	brokers := strings.Split("localhost:9002,localhost:9003", ",")
	ps.InitPublisher(brokers...)
	err := ps.InitConsumerGroup(cf.PubsubSubscription, brokers...)
	if err != nil {
		return err
	}
	return nil
}

```


* Publisher setup


``` go

func NewPubSub() error {
	ps := &psub.PubSub{}
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


func NewPubSub() error {
	ps := &psub.PubSub{}
	brokers := strings.Split("localhost:9002,localhost:9003", ",")
	err := ps.InitConsumerGroup(cf.PubsubSubscription, brokers...)
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



func onListenUserInsert(payload []byte, h *Account, fnCommit func()) {
	defer fnCommit()
	user := &User{}
    onCreateUser(user)
    ...
}

type MsgHandler struct {
	f          func([]byte, *Account, func())
	AutoCommit bool
}

var (
	backend_user_insert_user = "backend_user_insert_user"
    numberOfConcurrents = 1
	subsList                   = map[string]*MsgHandler{
		backend_user_insert_user:                   {onListenUserInsert, false},
	}
)

func onStreamRunning(ps *pubsub.PubSub, h *Account) {
	log.Print("Stream start")
	msgBuf := make(chan pubsub.Message, 1000)


	topics := make([]pubsub.Topic, 0)
	for topic, handler := range subsList {
		topics = append(topics, pubsub.Topic{Name: topic, AutoCommit: handler.AutoCommit})
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
