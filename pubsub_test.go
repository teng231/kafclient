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

//**************** Test send message to topic using sync **********************/
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

// ***** Test send message with config ****/
func TestPushMessageWithTopicConfig(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("0.0.0.0:9092", ",")

	ps.InitPublisher(brokers...)
	log.Print("init done publiser")
	// ************ Send message with header ***************
	config := &SenderConfig{
		Headers: map[string]string{"token": "123"},
	}
	topic := &Topic{Name: "cg_topic_test1"}
	for i := 1; i <= 20; i++ {
		err := ps.PublishWithConfig(topic, config, &User{
			Name: "nguoi dau tien",
			Age:  i,
		})
		log.Print(err)
		time.Sleep(100 * time.Millisecond)
	}
	// *************** Send message with specific partition ***********
	topic = &Topic{Name: "cg_topic_test2", AutoCommit: true, Partition: ToPInt32(1)}
	for i := 1; i <= 20; i++ {
		err := ps.PublishWithConfig(topic, config, &User{
			Name: "nguoi thu 2",
			Age:  i,
		})
		log.Print(err)
		time.Sleep(100 * time.Millisecond)
	}
}

//**************** Test handle message when listen event auto commit **********************/
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
				log.Print(d.Headers, d.Partition, user)
			}
		}
	}()
	err = ps.OnAsyncSubscribe([]*Topic{{
		Name: "cg_topic_test1", AutoCommit: true,
	}, {
		Name: "cg_topic_test2", AutoCommit: true, Partition: ToPInt32(1),
	},
	}, 1, buff)
	log.Print(err)
}

// ********** Test listen message when listen message and manual commit ****/
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
	err = ps.OnAsyncSubscribe([]*Topic{{
		"cg_topic_test1", false, nil,
	}}, 1, buff)
	log.Print(err)
}

func Test_listTopic(t *testing.T) {
	ps := &PubSub{}
	brokers := strings.Split("0.0.0.0:9092", ",")
	ts, err := ps.ListTopics(brokers...)
	log.Print(ts, err)
}

// **************************** Test with cluster **************************** /
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
	err = ps.OnAsyncSubscribe([]*Topic{{
		Name: "cg_topic_test1", AutoCommit: false,
	}}, 1, buff)
	log.Print(err)
}
