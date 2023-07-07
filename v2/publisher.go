package kafclient

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func (k *Client) IsReaderConnected() bool {
	if len(k.readers) == 0 {
		return false
	}
	return true
}
func (k *Client) NewPublisher() error {
	if len(k.addrs) == 0 {
		return errors.New("not found broker")
	}
	dialer := &kafka.Dialer{
		DualStack: true,
		Timeout:   1 * time.Second,
		// TLS:       &tls.Config{...tls config...},
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  k.addrs,
		Balancer: &kafka.RoundRobin{},
		Dialer:   dialer,
		// Logger:   kafka.LoggerFunc(log.Printf),
	})

	if w == nil {
		log.Print("empty writer")
	}
	log.Print("writer created")
	k.writer = w
	return nil
}

func (k *Client) Publish(ctx context.Context, topic string, msg interface{}) error {
	if !k.IsWriters() {
		return errors.New("writers not created")
	}
	if topic == "" {
		return errors.New("topic not empty")
	}
	dataSender, err := json.Marshal(msg)
	if err != nil {
		return errors.New("message of data sender can not marshal")
	}
	err = k.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(hashMessage(dataSender)),
		Value: dataSender,
	})
	return err
}

// func (k *Client) PublishAsync(ctx context.Context, topic string, msg interface{}) error {
// 	if !k.IsWriters() {
// 		return errors.New("writers not created")
// 	}
// 	if topic == "" {
// 		return errors.New("topic not empty")
// 	}
// 	dataSender, err := json.Marshal(msg)
// 	if err != nil {
// 		return errors.New("message of data sender can not marshal")
// 	}
// 	err = k.writer.WriteMessages(ctx, kafka.Message{
// 		Topic: topic,
// 		Key:   []byte(hashMessage(dataSender)),
// 		Value: dataSender,
// 	})
// 	return err
// }
