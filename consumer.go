package kafclient

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

func newConsumerGroup(consumerGroup string, reconnect chan bool, brokerURLs ...string) (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()
	// config.ClientID = clientid
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	// config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	// config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
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
			sarama.NewConsumerGroupFromClient(consumerGroup, client)
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
	consumer, err := sarama.NewConsumer(brokerURLs, config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

func (ps *Client) createTopic(topic string) error {
	config := sarama.NewConfig()
	config.Version = ps.kafkaVersion
	admin, err := sarama.NewClusterAdmin(ps.brokerURLs, config)
	if err != nil {
		log.Println("[warning]: ", err, ps.brokerURLs)
		return err
	}
	detail := &sarama.TopicDetail{
		NumPartitions:     int32(NUM_PARTITION),
		ReplicationFactor: int16(REPLICATION_FACTOR),
	}
	err = admin.CreateTopic(topic, detail, false)
	if err != nil {
		log.Println("[psub]:", err)
	}
	log.Print(detail)
	return err
}

func (ps *Client) InitConsumerGroup(consumerGroup string, brokerURLs ...string) error {
	client, err := newConsumerGroup(consumerGroup, ps.reconnect, brokerURLs...)
	if err != nil {
		return err
	}
	ps.group = client
	ps.brokerURLs = brokerURLs
	ps.consumerGroup = consumerGroup
	// ps.reconnect = make(chan bool)
	return nil
}
func (ps *Client) InitConsumer(brokerURLs ...string) error {
	client, err := newConsumer(brokerURLs...)
	if err != nil {
		return err
	}
	ps.consumer = client
	ps.brokerURLs = brokerURLs
	return nil
}

func (ps *Client) OnScanMessages(topics []string, bufMessage chan Message) error {
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

func BodyParse(bin []byte, p interface{}) error {
	return json.Unmarshal(bin, p)
}

func (ps *Client) ListTopics(brokers ...string) ([]string, error) {
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
func (ps *Client) OnAsyncSubscribe(topics []*Topic, numberPuller int, buf chan Message) error {
	// ps.onAsyncSubscribe(topics, numberPuller, buf)
	var err error
	for {
		err = ps.onAsyncSubscribe(topics, numberPuller, buf)
		if err != nil {
			log.Print(err)
		}
		time.Sleep(5 * time.Second)
		log.Print("try reconnecting ....")
		ps.InitConsumerGroup(ps.consumerGroup, ps.brokerURLs...)

	}
	// return err
}

// onAsyncSubscribe listener
func (ps *Client) onAsyncSubscribe(topics []*Topic, numberPuller int, buf chan Message) error {
	if len(topics) == 0 {
		log.Print("not found topics")
		return nil
	}
	txtTopics := []string{}
	autoCommit := map[string]bool{}
	allTopics, err := ps.ListTopics(ps.brokerURLs...)
	if err != nil {
		log.Print("can't not list topics existed")
		return err
	}
	mTopic := make(map[string]bool)
	for _, topic := range allTopics {
		mTopic[topic] = true
	}
	for _, topic := range topics {
		if strings.Contains(topic.Name, "__consumer_offsets") {
			continue
		}
		if topic.IsNeedManualCreateTopic {
			if _, has := mTopic[topic.Name]; has {
				ps.createTopic(topic.Name)
			}
		}

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
	consumer.wg.Add(numberPuller)
	for i := 0; i < numberPuller; i++ {
		go func() {
			defer consumer.wg.Done()
			for {
				// `Consume` should be called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				err := ps.group.Consume(ctx, txtTopics, consumer)
				if err != nil {
					log.Printf("[psub]: %v", err)
					consumer.lock <- true
					log.Print("con gi nua dau 4445555")

					break
				}
				// // check if context was cancelled, signaling that the consumer should stop
				// if ctx.Err() != nil {
				// 	log.Print("con gi nua dau 4444 ", ctx.Err())

				// 	return
				// }
				// consumer.lock = make(chan bool)
				// log.Print("con gi nua dau 222")

			}
		}()
	}

	log.Print("[kafka] start all worker")
	<-consumer.lock
	cancel()
	consumer.wg.Wait()
	if err := ps.group.Close(); err != nil {
		log.Printf("Error closing client: %v", err)
		return err
	}
	log.Print("con gi nua dau")
	return nil
}

func messageHandler(m *sarama.ConsumerMessage, bufMessage chan Message) error {
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
	if len(m.Headers) != 0 {
		headers := map[string]string{}
		for _, header := range m.Headers {
			headers[string(header.Key)] = string(header.Value)
		}
		msg.Headers = headers
	}
	bufMessage <- msg
	return nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *ConsumerGroupHandle) Setup(ss sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	log.Print("done: ", ss.MemberID())
	// close(consumer.lock)
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
	// https://github.com/IBM/sarama/blob/master/consumer_group.go#L27-L29
	for {
		select {
		case m := <-claim.Messages():
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
			if len(m.Headers) != 0 {
				headers := map[string]string{}
				for _, header := range m.Headers {
					headers[string(header.Key)] = string(header.Value)
				}
				msg.Headers = headers
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

			// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

func (ps *Client) Close() error {
	if ps.consumer != nil {
		if err := ps.consumer.Close(); err != nil {
			return err
		}
	}
	// var err error
	// ps.mProducer.Range(func(k interface{}, sp interface{}) bool {
	// 	if sp == nil {
	// 		return true
	// 	}
	// 	err = sp.(sarama.SyncProducer).Close()
	// 	if err != nil {
	// 		log.Print("close error: ", err.Error())
	// 	}
	// 	return true
	// })
	return nil
}
