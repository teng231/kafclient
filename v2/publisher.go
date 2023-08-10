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
	w := &kafka.Writer{
		Addr:         kafka.TCP(k.addrs...),
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}

	// if w == nil {
	// 	log.Print("empty writer")
	// }
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
	now := time.Now()
	err = k.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(hashMessage(dataSender)),
		Value: dataSender,
	})
	log.Print("x1 ", time.Since(now))
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
