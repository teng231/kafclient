package kafclient

import (
	"context"
	"crypto/md5"
	"encoding/hex"

	"github.com/segmentio/kafka-go"
)

// Message define message encode/decode sarama message
type Message struct {
	Offset        int64  `json:"offset,omitempty"`
	Partition     int    `json:"partition,omitempty"`
	Topic         string `json:"topic,omitempty"`
	Key           string `json:"key,omitempty"`
	Body          []byte `json:"body,omitempty"`
	Timestamp     int64  `json:"timestamp,omitempty"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
	Commit        func()
	Headers       map[string]string
}

func hashMessage(btext []byte) string {
	hash := md5.Sum(btext)
	return hex.EncodeToString(hash[:])
}

type Client struct {
	writer *kafka.Writer

	readers     map[string]*kafka.Reader
	addrs       []string
	group       *kafka.ConsumerGroup
	groupTopics []string // listen
}

func (k *Client) SetAddrs(addrs []string) {
	k.addrs = addrs
}

func (k *Client) HealthCheckBroker() {
	// client := kafka.NewReader()
	// k.addrs = addrs
}

type IClient interface {
	SetAddrs(addrs []string)
	Listen(ctx context.Context, cMgs chan *Message) error
	ListenWithAutoCommit(ctx context.Context, cMgs chan *Message) error
	NewConsumer(consumerGroup string, topics []string)
	IsWriters() bool

	NewPublisher() error
	Publish(ctx context.Context, topic string, msg interface{}) error
	IsReaderConnected() bool
}
