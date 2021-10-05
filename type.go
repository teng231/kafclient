package pubsub

import (
	"sync"

	"github.com/Shopify/sarama"
)

var (
	NUM_PARTITION      = 3
	REPLICATION_FACTOR = 1
	// publisherConfig    *sarama.Config
	kafkaVersion = "2.5.0"
)

// SenderConfig addion config when publish message
type SenderConfig struct {
	Metadata interface{}
	Headers  map[string]string
}
type PubSub struct {
	brokerURLs []string
	producers  map[string]sarama.SyncProducer
	group      sarama.ConsumerGroup
	lock       *sync.Mutex
	addrs      []string

	consumer sarama.Consumer // for using consumer mode
}

type IPubsub interface {
	InitConsumerGroup(consumerGroup string, brokerURLs ...string) error
	InitConsumer(brokerURLs ...string) error // depredicated
	InitPublisher(brokerURLs ...string)      // backward compatible
	// Publish send multiple messages to topic
	Publish(topic string, messages ...interface{}) error
	OnScanMessages(topics []string, bufMessage chan Message) error // depredicated
	// ListTopics for ping
	ListTopics(brokers ...string) ([]string, error)
	OnAsyncSubscribe(topics []*Topic, numberworkers int, buf chan Message) error
	// PublishWithConfig help we can publish message to 1 partition.
	// help application process task synchronized
	PublishWithConfig(topic *Topic, config *SenderConfig, messages ...interface{}) error
}

// Message define message encode/decode sarama message
type Message struct {
	Offset        int64  `json:"offset,omitempty"`
	Partition     int    `json:"partition,omitempty"`
	Topic         string `json:"topic,omitempty"`
	Body          []byte `json:"body,omitempty"`
	Timestamp     int64  `json:"timestamp,omitempty"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
	Commit        func()
	// new property
	Headers map[string]string
}

// ConsumerGroupHandle represents a Sarama consumer group consumer
type ConsumerGroupHandle struct {
	wg         *sync.WaitGroup
	lock       chan bool
	bufMessage chan Message
	autoCommit map[string]bool
}
