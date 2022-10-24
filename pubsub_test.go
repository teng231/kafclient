package kafclient

import (
	"log"
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

func TestNormalCaseSync(t *testing.T) {
	ps := &Client{}
	brokers := strings.Split("0.0.0.0:9092", ",")

	ps.InitPublisher(brokers...)
	log.Print("init done publiser")
	for i := 1; i <= 30; i++ {
		err := ps.Publish("topic-0", &User{
			Name: "hoa",
			Age:  i,
		})
		log.Print(i, err)
		time.Sleep(100 * time.Millisecond)
	}
	defer ps.Close()
	err := ps.InitConsumerGroup("CG-0", brokers...)
	if err != nil {
		log.Print(err)
		return
	}
	log.Print("init done consumer")
	buff := make(chan Message, 20)
	go func() {
		for {
			d := <-buff
			user := &User{}
			BodyParse(d.Body, user)
			log.Print(d.Headers, d.Partition, user, " ", d.Partition)
			d.Commit()
		}
	}()
	err = ps.OnAsyncSubscribe([]*Topic{{
		"topic-0", false, nil, false,
	}}, 1, buff)
	log.Print(err)
}

func testPublishMessages(brokerURL, topic string, count int) {
	ps := &Client{}
	brokers := strings.Split(brokerURL, ",")

	ps.InitPublisher(brokers...)
	log.Print("init done publiser")
	for i := 1; i <= count; i++ {
		err := ps.Publish(topic, &User{
			Name: "hoa",
			Age:  i,
		})
		log.Print(i, err)
	}
	ps.Close()
}

func TestPuslishMessages(t *testing.T) {
	testPublishMessages("0.0.0.0:9092", "topic-0", 20)
}

// ***** Test send message with config ****/
func testPuslishMessagesWithConfig(brokerURL, topic1, topic2 string) {
	ps := &Client{}
	brokers := strings.Split(brokerURL, ",")

	ps.InitPublisher(brokers...)
	config := &SenderConfig{
		Headers: map[string]string{"token": "abc123"},
	}
	topic := &Topic{Name: topic1}
	for i := 1; i <= 20; i++ {
		err := ps.PublishWithConfig(topic, config, &User{
			Name: "nguoi dau tien",
			Age:  i,
		})
		log.Print(i, err)
		time.Sleep(100 * time.Millisecond)
	}
	// *************** Send message with specific partition ***********
	topic = &Topic{Name: topic2, AutoCommit: true, Partition: ToPInt32(1)}
	for i := 1; i <= 20; i++ {
		err := ps.PublishWithConfig(topic, config, &User{
			Name: "nguoi thu 2",
			Age:  i,
		})
		log.Print(err)
		time.Sleep(100 * time.Millisecond)
	}
}
func TestPuslishMessagesWithConfig(t *testing.T) {
	testPuslishMessagesWithConfig("0.0.0.0:9092", "topic-1", "topic-2")
}

//**************** Test handle message when listen event auto commit **********************/
func testSubscribeSimpleAutoCommit(brokerURL, group, topic1, topic2 string) {
	ps := &Client{}
	brokers := strings.Split(brokerURL, ",")

	err := ps.InitConsumerGroup(group, brokers...)
	if err != nil {
		log.Print(err)
		return
	}
	log.Print("init done consumer")
	buff := make(chan Message, 5)
	go func() {
		for {
			d := <-buff
			user := &User{}
			BodyParse(d.Body, user)
			log.Print(d.Headers, d.Partition, user)
		}
	}()
	err = ps.OnAsyncSubscribe([]*Topic{
		{
			Name: topic1, AutoCommit: true,
		}, {
			Name: topic2, AutoCommit: true, Partition: ToPInt32(1),
		},
	}, 1, buff)
	log.Print(err)
}
func TestSubscribeSimpleAutoCommit(t *testing.T) {
	testSubscribeSimpleAutoCommit("0.0.0.0:9092", "CG-0", "topic-1", "topic-2")
}

// ********** Test listen message when listen message and manual commit ****/
func testSubscribeSimpleManualCommit(brokerURL, group, topic string) {
	ps := &Client{}
	brokers := strings.Split(brokerURL, ",")

	err := ps.InitConsumerGroup(group, brokers...)
	if err != nil {
		log.Print(err)
		return
	}
	log.Print("init done consumer")
	buff := make(chan Message, 20)
	go func() {
		for {
			d := <-buff
			user := &User{}
			BodyParse(d.Body, user)
			log.Print(d.Headers, d.Partition, user)
			if user.Age != 100 {
				d.Commit()
			}
		}
	}()
	err = ps.OnAsyncSubscribe([]*Topic{{
		topic, false, nil, false,
	}}, 1, buff)
	log.Print(err)
}
func TestSubscribeSimpleManualCommit(t *testing.T) {
	testSubscribeSimpleManualCommit("0.0.0.0:9092", "CG-0", "topic-0")
}

func TestListTopic(t *testing.T) {
	ps := &Client{}
	brokers := strings.Split("0.0.0.0:9092", ",")
	ts, err := ps.ListTopics(brokers...)
	log.Print(ts, err)
}

// --------------------- Test full publish and subscribe ---------------------

func TestSimplePublishAndSubscibe(t *testing.T) {
	lock := make(chan bool)
	go t.Run("subscribe 1", func(t *testing.T) {
		testSubscribeSimpleManualCommit("0.0.0.0:9092", "CG-0", "topic-3")
	})
	go t.Run("subscribe 2", func(t *testing.T) {
		testSubscribeSimpleManualCommit("0.0.0.0:9092", "CG-1", "topic-3")
	})
	time.Sleep(5 * time.Second)
	t.Run("publish to topic", func(t *testing.T) {
		testPublishMessages("0.0.0.0:9092", "topic-3", 20)
	})
	<-lock
}

// ------------------- Benchmark test ----------------
func BenchmarkPublishMessages100z(b *testing.B) {
	testPublishMessages("0.0.0.0:9092", "topic-4", 100)
}

func BenchmarkPublishMessages10000z(b *testing.B) {
	testPublishMessages("0.0.0.0:9092", "topic-5", 10000)
}

func BenchmarkSubscribeSimpleManualCommit(b *testing.B) {
	testSubscribeSimpleManualCommit("0.0.0.0:9092", "CG-1", "topic-5")
}
