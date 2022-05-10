package kafclient

import (
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

func makeKafkaConfigPublisher(partitioner sarama.PartitionerConstructor) *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	// config.Producer.Partitioner = sarama.NewManualPartitioner()
	config.Producer.Partitioner = partitioner
	// publisherConfig = config
	return config
}

func newPublisher(topic string, brokerURLs ...string) (sarama.SyncProducer, error) {
	config := makeKafkaConfigPublisher(sarama.NewRandomPartitioner)
	prd, err := sarama.NewSyncProducer(brokerURLs, config)
	if err != nil {
		return nil, err
	}
	return prd, nil
}

func newAsyncPublisher(topic string, brokerURLs ...string) (sarama.AsyncProducer, error) {
	config := makeKafkaConfigPublisher(sarama.NewRandomPartitioner)
	prd, err := sarama.NewAsyncProducer(brokerURLs, config)
	if err != nil {
		return nil, err
	}
	return prd, nil
}

func newPublisherWithConfigPartitioner(topic *Topic, brokerURLs ...string) (sarama.SyncProducer, error) {
	var config *sarama.Config
	if topic.Partition == nil {
		config = makeKafkaConfigPublisher(sarama.NewRandomPartitioner)
	} else if ToInt32(topic.Partition) >= 0 {
		config = makeKafkaConfigPublisher(sarama.NewManualPartitioner)
	}
	prd, err := sarama.NewSyncProducer(brokerURLs, config)
	if err != nil {
		return nil, err
	}
	return prd, nil
}

// InitPublisher init with addr is url of lookupd
func (ps *KafClient) InitPublisher(brokerURLs ...string) {
	ps.brokerURLs = brokerURLs
	// ps.producers = make(map[string]sarama.SyncProducer)
}

// Publish sync publish message
func (p *KafClient) Publish(topic string, messages ...interface{}) error {
	if strings.Contains(topic, "__consumer_offsets") {
		return errors.New("topic fail")
	}

	if _, ok := p.mProducer.Load(topic); !ok {
		producer, err := newPublisher(topic, p.brokerURLs...)
		if err != nil {
			log.Print("[sarama]:", err, "]: topic", topic)
			return err
		}
		p.mProducer.Store(topic, producer)
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
		// asyncProducer.(sarama.AsyncProducer).Input() <- msg
		listMsg = append(listMsg, msg)
	}
	syncProducer, ok := p.mProducer.Load(topic)
	if !ok {
		log.Print("not found any sync Producer")
	}
	err := syncProducer.(sarama.SyncProducer).SendMessages(listMsg)
	return err
}

// AsyncPublish async publish message
func (p *KafClient) AsyncPublish(topic string, messages ...interface{}) error {
	if strings.Contains(topic, "__consumer_offsets") {
		return errors.New("topic fail")
	}

	if _, ok := p.mProducer.Load(topic); !ok {
		producer, err := newAsyncPublisher(topic, p.brokerURLs...)
		if err != nil {
			log.Print("[sarama]:", err, "]: topic", topic)
			return err
		}
		p.mProducer.Store(topic, producer)
	}
	asyncProducer, _ := p.mProducer.Load(topic)
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
		asyncProducer.(sarama.AsyncProducer).Input() <- msg
	}
	return nil
}

// PublishWithConfig sync publish message with select config
// Sender config help config producerMessage
func (p *KafClient) PublishWithConfig(topic *Topic, config *SenderConfig, messages ...interface{}) error {
	if strings.Contains(topic.Name, "__consumer_offsets") {
		return errors.New("topic fail")
	}
	if _, ok := p.mProducer.Load(topic); !ok {
		producer, err := newPublisherWithConfigPartitioner(topic, p.brokerURLs...)
		if err != nil {
			log.Print("[sarama]:", err, "]: topic", topic)
			return err
		}
		p.mProducer.Store(topic, producer)
	}
	listMsg := make([]*sarama.ProducerMessage, 0)
	for _, msg := range messages {
		bin, err := json.Marshal(msg)
		if err != nil {
			log.Printf("[sarama] error: %v[sarama] msg: %v", err, msg)
		}
		pmsg := &sarama.ProducerMessage{
			Topic:     topic.Name,
			Value:     sarama.StringEncoder(bin),
			Partition: -1,
		}
		if topic.Partition != nil {
			pmsg.Partition = ToInt32(topic.Partition)
		}

		if config != nil {
			pmsg.Metadata = config.Metadata
			if len(config.Headers) > 0 {
				for k, v := range config.Headers {
					pmsg.Headers = append(pmsg.Headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
				}
			}
		}
		listMsg = append(listMsg, pmsg)

	}
	syncProducer, ok := p.mProducer.Load(topic)
	if !ok {
		log.Print("not found any sync Producer")
	}
	err := syncProducer.(sarama.SyncProducer).SendMessages(listMsg)
	return err
}
