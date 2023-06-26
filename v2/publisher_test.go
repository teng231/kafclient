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
func TestListenMessage2(t *testing.T) {
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
	time.Sleep(10 * time.Second)
}
