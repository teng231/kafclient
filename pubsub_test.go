package pubsub

import (
	"log"
	"os"
	"strings"
	"time"

	"testing"
)

type User struct {
	Name string
	Age  int
}

/*
	Test with consumer group:
	* case 1: 2 consumer group
		publisher: go test -run TestCGPush2Message
		consumer: CG=cg1 go test -run TestCGHandleMessage | CG=cg2 go test -run TestCGHandleMessage
	* case 2: 1 consumer group
		publisher: go test -run TestCGPush2Message
		consumer: CG=cg1 go test -run TestCGHandleMessage | CG=cg1 go test -run TestCGHandleMessage

	Test with consumer and publisher

*/
func TestCGPush2Message(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("0.0.0.0:9092", ",")

	ps.InitPublisher(brokers...)
	log.Print("init done publiser")
	for i := 1; i <= 100; i++ {
		err := ps.Publish("cg_topic_test1", &User{
			Name: "hoa",
			Age:  i,
		})
		log.Print(err)
		time.Sleep(100 * time.Millisecond)
	}
}

func TestCGHandleMessage(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("0.0.0.0:9092", ",")

	err := ps.InitConsumerGroup(os.Getenv("CG"), brokers...)
	if err != nil {
		log.Print(err)
		return
	}
	log.Print("init done consumer")
	buff := make(chan Message, 5)
	go func() {
		for {
			select {
			case d := <-buff:
				user := &User{}
				BodyParse(d.Body, user)
				log.Print(user)
			}
		}
	}()
	err = ps.OnAsyncSubscribe([]Topic{{
		"cg_topic_test1", true,
	}}, 1, buff)
	log.Print(err)
}
func TestCGHandleMessageWithManual(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("0.0.0.0:9092", ",")

	err := ps.InitConsumerGroup(os.Getenv("CG"), brokers...)
	if err != nil {
		log.Print(err)
		return
	}
	log.Print("init done consumer")
	buff := make(chan Message, 5)
	go func() {
		for {
			select {
			case d := <-buff:
				user := &User{}
				BodyParse(d.Body, user)
				log.Print(user)
				// d.Commit()
				if user.Age != 100 {
					d.Commit()
				}
			}
		}
	}()
	err = ps.OnAsyncSubscribe([]Topic{{
		"cg_topic_test1", false,
	}}, 1, buff)
	log.Print(err)
}

func TestPubliserPushMessage(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("0.0.0.0:9092", ",")

	ps.InitPublisher(brokers...)
	log.Print("init done publiser")
	for i := 0; i < 100; i++ {
		err := ps.Publish("topic_test2", &User{
			Name: "linh",
			Age:  i,
		})
		log.Print(err, i)
		time.Sleep(300 * time.Millisecond)
	}
}

func TestConsumeHandleMessage(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("0.0.0.0:9092", ",")
	ps.InitConsumer(brokers...)
	buff := make(chan Message, 5)
	go func() {
		for {
			select {
			case d := <-buff:
				user := &User{}
				BodyParse(d.Body, user)
				log.Print("received:", user)
			}

		}
	}()
	err := ps.OnScanMessages([]string{"topic_test2"}, buff)
	log.Print(err)
}
func Test_listTopic(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("0.0.0.0:9092", ",")
	ts, err := ps.ListTopics(brokers...)
	log.Print(ts, err)
}

func Test_listTopic2(t *testing.T) {
	ps := &PubSub{}
	// brokers := strings.Split("10.130.236.212:9092", ",")
	err := ps.InitConsumerGroup("any", "0.0.0.0:9092")
	log.Print(err)
	// ts, err := ps.ListTopics(brokers...)
	// log.Print(ts, err)
}

func TestCGPush2Message2(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("localhost:9002,localhost:9003", ",")

	ps.InitPublisher(brokers...)
	log.Print("init done publiser")
	for i := 1; i <= 100; i++ {
		err := ps.Publish("cg_topic_test1", &User{
			Name: "hoa",
			Age:  i,
		})
		log.Print(err)
		time.Sleep(100 * time.Millisecond)
	}
}

func TestCGHandleMessage2(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("localhost:9002,localhost:9003", ",")

	err := ps.InitConsumerGroup(os.Getenv("CG"), brokers...)
	if err != nil {
		log.Print(err)
		return
	}
	log.Print("init done consumer")
	buff := make(chan Message, 5)
	go func() {
		for {
			select {
			case d := <-buff:
				user := &User{}
				BodyParse(d.Body, user)
				log.Print(user)
				d.Commit()
			}
		}
	}()
	err = ps.OnAsyncSubscribe([]Topic{{
		"cg_topic_test1", false,
	}}, 1, buff)
	log.Print(err)
}
