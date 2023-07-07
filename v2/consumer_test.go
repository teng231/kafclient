package kafclient

import (
	"context"
	"log"
	"testing"
	"time"
)

func TestListenMessageAutoCommit(t *testing.T) {
	kclient := &Client{}
	kclient.SetAddrs([]string{"localhost:9092"})
	kclient.NewConsumer("tete1", []string{"topic-1", "topic-2"})
	cmsg := make(chan *Message, 1000)
	log.Print("Listen message 1")
	kclient.ListenWithAutoCommit(context.Background(), cmsg)
	log.Print("Listen message 2")
	for msg := range cmsg {
		log.Print(string(msg.Body))
	}
	time.Sleep(10 * time.Second)
}
func TestListenMessageManual(t *testing.T) {
	kclient := &Client{}
	kclient.SetAddrs([]string{"localhost:9092"})
	kclient.NewConsumer("tete2", []string{"topic-1", "topic-2"})
	cmsg := make(chan *Message, 1000)
	log.Print("Listen message 1")
	kclient.Listen(context.Background(), cmsg)
	log.Print("Listen message 2")
	for msg := range cmsg {
		log.Print(msg, string(msg.Body))
		msg.Commit()
	}
	time.Sleep(10 * time.Second)
}

// go test --run TestListenMessageManual

func TestListenMessageManual2(t *testing.T) {
	kclient := &Client{}
	kclient.SetAddrs([]string{"b-1.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092", "b-2.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092"})
	kclient.NewConsumer("tete2", []string{"topic-0", "topic-1", "topic-2",
		"topic-3",
		"topic-4x",
		"topic-5x",
		"topic-6x",
	})
	cmsg := make(chan *Message, 1000)
	log.Print("Listen message 1")
	kclient.Listen(context.Background(), cmsg)
	log.Print("Listen message 2")
	for msg := range cmsg {
		// b, _ := json.MarshalIndent(msg, "", " ")
		log.Printf("[%s] - part: %d - %s", msg.Topic, msg.Partition, string(msg.Body))
		log.Print(msg)
		msg.Commit()
	}
	time.Sleep(10 * time.Second)
}
