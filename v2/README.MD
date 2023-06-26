## Kafka client Version 2

New kafclient implement lib kafka-go instead of sarama

### WHY WE NEED CHANGE IT?

- Sarama is good, so fast for subscribe message, but it need more config but so confused.
- Connect to broker not stable. Can not reconnect with broker restart, we manual reconnect.
- Configuration so many steps, but not have timeout connect, not have health check.
- Lib change and breaking change many, so we implement has bugs.

then we restructure this flow and change base lib.


### Installation

```bash
    go get github.com/teng231/kafclient/v2
```

### Using

+ Start with mod consumer:
```go
    kclient := &Client{}
    kclient.SetAddrs([]string{"localhost:9092"})
    kclient.NewPublisher()
    err := kclient.Publish(context.TODO(), "topic-1", map[string]interface{}{
            "meta":  "tester2",
            "index": 1,
            "topic": "topic-1",
        })
```
+ Start with mod producer
```go
    kclient := &Client{}
	kclient.SetAddrs([]string{"localhost:9092"})
	kclient.NewConsumer("tete2", []string{"topic-1", "topic-2"})
	cmsg := make(chan *Message, 1000)
	log.Print("Listen message 1")
	kclient.Listen(context.Background(), cmsg)
	log.Print("Listen message 2")
	for msg := range cmsg {
		log.Print(string(msg.Body))
		msg.Commit()
	}
```