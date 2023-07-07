package kafclient

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"regexp"
	"time"

	"github.com/segmentio/kafka-go"
	topics "github.com/segmentio/kafka-go/topics"
)

func newClient(addr net.Addr) *kafka.Client {

	// transport := &kafka.Transport{
	// 	Dial:     conns.Dial,
	// 	Resolver: kafka.NewBrokerResolver(nil),
	// }

	client := &kafka.Client{
		Addr:    addr,
		Timeout: 5 * time.Second,
		// Transport: transport,
	}

	return client
}

func (k *Client) ListTopics() {
	client := newClient(kafka.TCP(k.addrs...))
	topics, err := topics.List(context.TODO(), client)
	if err != nil {
		log.Print(err)
		return
	}
	b, _ := json.MarshalIndent(topics, "", " ")
	log.Print(string(b))

}

func (k *Client) ListTopics2() {
	client := newClient(kafka.TCP(k.addrs...))
	r := regexp.MustCompile("topic-.*")
	topics, err := topics.ListRe(context.TODO(), client, r)
	if err != nil {
		log.Print(err)
		return
	}
	b, _ := json.MarshalIndent(topics, "", " ")
	log.Print(string(b))

}

func (k *Client) CreateTopic(topic string, numPart int) {
	client := newClient(kafka.TCP(k.addrs...))
	resp, err := client.CreateTopics(context.TODO(), &kafka.CreateTopicsRequest{
		Addr: kafka.TCP(k.addrs...),
		Topics: []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     numPart,
				ReplicationFactor: 2,
			},
		},
	})
	if err != nil {
		log.Print(err)
	}
	log.Print(resp)
}
