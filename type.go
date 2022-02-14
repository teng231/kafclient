package pubsub

import (
	"sync"

	"github.com/Shopify/sarama"
)

var (
	NUM_PARTITION      = 3
	REPLICATION_FACTOR = 1
	kafkaVersion       = "2.5.0"
)

// SenderConfig addion config when publish message
type SenderConfig struct {
	Metadata interface{}
	Headers  map[string]string
}

type Topic struct {
	Name       string
	AutoCommit bool
	Partition  *int32
}

type PubSubConfig struct {
	KafkaVersion           string // default : 2.5.0
	BrokerURLs             []string
	KafkaNumerberPartition int // default: using 3 partitions
	KafkaReplicationFactor int // default: -1
}

type PubSub struct {
	brokerURLs   []string
	mProducer    sync.Map
	group        sarama.ConsumerGroup
	consumer     sarama.Consumer // for using consumer mode
	kafkaVersion sarama.KafkaVersion
}

type IPubsub interface {
	InitConsumerGroup(consumerGroup string, brokerURLs ...string) error
	// InitConsumer depredicated
	InitConsumer(brokerURLs ...string) error
	InitPublisher(brokerURLs ...string) // backward compatible
	// Publish send multiple messages to topic
	Publish(topic string, messages ...interface{}) error
	// OnScanMessages depredicated
	OnScanMessages(topics []string, bufMessage chan Message) error
	// ListTopics for ping
	ListTopics(brokers ...string) ([]string, error)
	// OnAsyncSubscribe subscribe message from list topics,
	// numberPuller is number worker goroutines for pull message from kafka server to message chan
	OnAsyncSubscribe(topics []*Topic, numberPuller int, buf chan Message) error
	// PublishWithConfig help we can publish message to 1 partition.
	// help application process task synchronized
	PublishWithConfig(topic *Topic, config *SenderConfig, messages ...interface{}) error
	Close() error
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
	Headers       map[string]string
}

// ConsumerGroupHandle represents a Sarama consumer group consumer
type ConsumerGroupHandle struct {
	wg         *sync.WaitGroup
	lock       chan bool
	bufMessage chan Message
	autoCommit map[string]bool
}
