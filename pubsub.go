package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type Message struct {
	Offset        int64  `json:"offset,omitempty"`
	Partition     int    `json:"partition,omitempty"`
	Topic         string `json:"topic,omitempty"`
	Body          []byte `json:"body,omitempty"`
	Timestamp     int64  `json:"timestamp,omitempty"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
	Commit        func()
}

func init() {
	log.SetFlags(log.Lshortfile)
	if num, err := strconv.Atoi(os.Getenv("KAFKA_NUM_PARTITION")); err == nil {
		NUM_PARTITION = num
	}
	if num, err := strconv.Atoi(os.Getenv("KAFKA_REPLICATION_FACTOR")); err == nil {
		REPLICATION_FACTOR = num
	}
	if os.Getenv("KAFKA_VERSION") != "" {
		kafkaVersion = os.Getenv("KAFKA_VERSION")
	}
}

type PubSub struct {
	brokerURLs []string
	producers  map[string]sarama.SyncProducer
	group      sarama.ConsumerGroup
	lock       *sync.Mutex
	addrs      []string

	consumer sarama.Consumer // for using consumer mode
}

var (
	NUM_PARTITION      = 3
	REPLICATION_FACTOR = 1
	publisherConfig    *sarama.Config
	kafkaVersion       = "2.5.0"
)

func makeKafkaConfigPublisher() {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	publisherConfig = config
}

func newConsumerGroup(consumerGroup string, brokerURLs ...string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	// config.ClientID = clientid
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	// config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		log.Printf("Error parsing Kafka version: %v", err)
		return nil, err
	}
	config.Version = version
	// config.Producer.Partitioner =
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Start with a client
	client, err := sarama.NewClient(brokerURLs, config)
	if err != nil {
		return nil, err
	}

	// Start a new consumer group
	group, err := sarama.NewConsumerGroupFromClient(consumerGroup, client)
	if err != nil {
		client.Close()
		return nil, err
	}
	go func() {
		for err := range group.Errors() {
			log.Println("ERROR:", err)
		}
	}()
	return group, nil
}

func newConsumer(brokerURLs ...string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		log.Printf("Error parsing Kafka version: %v", err)
		return nil, err
	}
	config.Version = version
	// config.Producer.Partitioner =
	// config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 500 * time.Millisecond
	// Start with a client
	// client, err := sarama.NewClient(brokerURLs, config)
	// if err != nil {
	// 	client.Close()
	// 	return nil, err
	// }
	consumer, err := sarama.NewConsumer(brokerURLs, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func newPublisher(topic string, brokerURLs ...string) (sarama.SyncProducer, error) {
	prd, err := sarama.NewSyncProducer(brokerURLs, publisherConfig)
	if err != nil {
		return nil, err
	}
	return prd, nil
}

func newAsyncPublisher(topic string, brokerURLs ...string) (sarama.AsyncProducer, error) {
	prd, err := sarama.NewAsyncProducer(brokerURLs, publisherConfig)
	if err != nil {
		return nil, err
	}
	return prd, nil
}

func (ps *PubSub) createTopic(topic string) error {
	config := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion("2.5.0")
	if err != nil {
		log.Printf("Error parsing Kafka version: %v", err)
		return err
	}
	config.Version = version
	admin, err := sarama.NewClusterAdmin(ps.addrs, config)
	if err != nil {
		log.Println("error is", err)
		return err
	}
	detail := &sarama.TopicDetail{
		NumPartitions:     int32(NUM_PARTITION),
		ReplicationFactor: int16(REPLICATION_FACTOR),
	}
	err = admin.CreateTopic(topic, detail, false)
	if err != nil {
		log.Println("error is", err)
	}
	log.Print(detail)
	return err
}

func (ps *PubSub) InitConsumerGroup(consumerGroup string, brokerURLs ...string) error {
	client, err := newConsumerGroup(consumerGroup, brokerURLs...)
	if err != nil {
		return err
	}
	ps.group = client
	ps.addrs = brokerURLs
	return nil
}
func (ps *PubSub) InitConsumer(brokerURLs ...string) error {
	client, err := newConsumer(brokerURLs...)
	if err != nil {
		return err
	}
	ps.consumer = client
	ps.addrs = brokerURLs
	return nil
}

// InitPublisher init with addr is url of lookupd
func (ps *PubSub) InitPublisher(brokerURLs ...string) {
	ps.brokerURLs = brokerURLs
	ps.lock = &sync.Mutex{}
	ps.producers = make(map[string]sarama.SyncProducer)
	ps.addrs = brokerURLs
	makeKafkaConfigPublisher()
}

func (p *PubSub) Publish(topic string, messages ...interface{}) error {
	if strings.Contains(topic, "__consumer_offsets") {
		return errors.New("topic fail")
	}
	if _, ok := p.producers[topic]; !ok {
		p.lock.Lock()
		producer, err := newPublisher(topic, p.brokerURLs...)
		if err != nil {
			log.Print("[sarama]:", err, "]: topic", topic)
			p.lock.Unlock()
			return err
		}
		p.producers[topic] = producer
		p.lock.Unlock()
	}
	listMsg := make([]*sarama.ProducerMessage, 0)
	for _, msg := range messages {
		bin, err := json.Marshal(msg)
		if err != nil {
			log.Printf("[sarama] error: %v[sarama] msg: %v", err, msg)
		}
		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Value:     sarama.StringEncoder(bin),
			Partition: -1,
		}
		listMsg = append(listMsg, msg)
	}
	err := p.producers[topic].SendMessages(listMsg)
	return err
}

func BodyParse(bin []byte, p interface{}) error {
	return json.Unmarshal(bin, p)
}

func (ps *PubSub) OnScanMessages(topics []string, bufMessage chan Message) error {
	done := make(chan bool)
	for _, topic := range topics {
		if strings.Contains(topic, "__consumer_offsets") {
			continue
		}
		partitions, _ := ps.consumer.Partitions(topic)
		log.Printf("number partitions: %v", len(partitions))

		// this only consumes partition no 1, you would probably want to consume all partitions
		for _, partition := range partitions {
			partitionConsumer, err := ps.consumer.ConsumePartition(topic, partition, 0)
			if nil != err {
				log.Printf("Topic %v Partitions: %v", topic, partition)
				continue
			}
			defer func() {
				if err := partitionConsumer.Close(); err != nil {
					log.Print(err)
				}
			}()
			go func(topic string, partitionConsumer sarama.PartitionConsumer) {
				for {
					select {
					case consumerError := <-partitionConsumer.Errors():
						log.Print(consumerError.Err)
						done <- true
					case msg := <-partitionConsumer.Messages():
						messageHandler(msg, bufMessage)
						partitionConsumer.HighWaterMarkOffset()
					}
				}
			}(topic, partitionConsumer)
		}
	}
	<-done
	return nil
}

func (ps *PubSub) ListTopics(brokers ...string) ([]string, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	cluster, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	defer func() {
		cluster.Close()
		config = nil
	}()
	return cluster.Topics()
}

type Topic struct {
	Name       string
	AutoCommit bool
}

// OnAsyncSubscribe listener
func (ps *PubSub) OnAsyncSubscribe(topics []Topic, numberworkers int, buf chan Message) error {
	txtTopics := []string{}
	autoCommit := map[string]bool{}
	for _, topic := range topics {
		if strings.Contains(topic.Name, "__consumer_offsets") {
			continue
		}
		ps.createTopic(topic.Name)
		txtTopics = append(txtTopics, topic.Name)
		autoCommit[topic.Name] = topic.AutoCommit
		if topic.AutoCommit {
			log.Print("don't forget commit topic: ", topic.Name)
		}
	}
	consumer := &ConsumerGroupHandle{
		wg:         &sync.WaitGroup{},
		bufMessage: buf,
		lock:       make(chan bool),
		autoCommit: autoCommit,
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer.wg.Add(numberworkers)

	for i := 0; i < numberworkers; i++ {
		go func() {
			for {
				// `Consume` should be called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				err := ps.group.Consume(ctx, txtTopics, consumer)
				if err != nil {
					log.Printf("Error from consumer: %v", err)
					break
				}
				consumer.wg = &sync.WaitGroup{}
				consumer.wg.Add(numberworkers)
			}
		}()
	}

	consumer.wg.Wait()
	log.Print("[kafka] start all worker")
	<-consumer.lock
	select {
	case err := <-ps.group.Errors():
		log.Print(err)
	}
	cancel()
	if err := ps.group.Close(); err != nil {
		log.Printf("Error closing client: %v", err)
		return err
	}
	return nil
}

func messageHandler(m *sarama.ConsumerMessage, bufMessage chan Message) error {
	if len(m.Value) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return errors.New("message error")
	}
	bufMessage <- Message{
		Topic:     m.Topic,
		Body:      m.Value,
		Offset:    m.Offset,
		Partition: int(m.Partition),
		Timestamp: m.Timestamp.Unix(),
	}
	return nil
}

// ConsumerGroupHandle represents a Sarama consumer group consumer
type ConsumerGroupHandle struct {
	wg         *sync.WaitGroup
	lock       chan bool
	bufMessage chan Message
	autoCommit map[string]bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *ConsumerGroupHandle) Setup(ss sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	log.Print("done: ", ss.MemberID())
	consumer.wg.Done()
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *ConsumerGroupHandle) Cleanup(ss sarama.ConsumerGroupSession) error {
	log.Print("sarama clearuppp: ", ss.MemberID())
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *ConsumerGroupHandle) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for m := range claim.Messages() {
		// log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s, partion = %v", string(message.Value), message.Timestamp, message.Topic, message.Partition)
		// messageHandler(message, consumer.bufMessage, session)
		if len(m.Value) == 0 {
			// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
			return errors.New("message error")
		}
		msg := Message{
			Topic:     m.Topic,
			Body:      m.Value,
			Offset:    m.Offset,
			Partition: int(m.Partition),
			Timestamp: m.Timestamp.Unix(),
		}
		if consumer.autoCommit[m.Topic] {
			session.MarkOffset(m.Topic, m.Partition, m.Offset, "")
			session.MarkMessage(m, "")
			consumer.bufMessage <- msg
			msg.Commit = func() {}
			continue
		}
		msg.Commit = func() {
			session.MarkOffset(m.Topic, m.Partition, m.Offset, "")
			session.MarkMessage(m, "")
		}
		consumer.bufMessage <- msg
	}
	return nil
}
