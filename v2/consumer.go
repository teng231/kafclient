package kafclient

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
func (k *Client) IsWriters() bool {
	if k.writer == nil {
		return false
	}
	return true
}
func (k *Client) Close() error {
	for _, r := range k.readers {
		r.Close()
	}
	return nil
}

func (k *Client) NewConsumer(consumerGroup string, topics []string) {
	batchSize := int(10e6) // 10MB
	dialer := &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true,
		KeepAlive: 5 * time.Second,
		ClientID:  RandStringBytes(5),
	}
	k.readers = make(map[string]*kafka.Reader)
	for _, topic := range topics {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  k.addrs,
			GroupID:  consumerGroup,
			Topic:    topic,
			Dialer:   dialer,
			MaxBytes: batchSize,
		})
		if r == nil {
			log.Print("empty reader")
		}
		log.Printf("Listen: %s, %d, [%s]", r.Stats().Partition, r.Stats().QueueCapacity, r.Stats().Topic)
		k.readers[topic] = r
	}
}

// Listen manual listen
// need call msg.Commit() when process done
// recommend for this process
func (k *Client) Listen(ctx context.Context, cMgs chan *Message) error {
	for _, r := range k.readers {
		go func(r *kafka.Reader) {
			for {
				m, err := r.FetchMessage(ctx) // is not auto commit
				if err != nil && errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				if err != nil && errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					log.Print(err)
					continue
				}
				cMgs <- &Message{
					Offset:    m.Offset,
					Partition: m.Partition,
					Topic:     m.Topic,
					Body:      m.Value,
					Timestamp: m.Time.Unix(),
					Key:       string(m.Key),
					Commit: func() {
						if err := r.CommitMessages(ctx, m); err != nil {
							log.Print("failed to commit messages:", err)
						}
					},
				}
			}
		}(r)
	}
	return nil
}

// ListenWithAutoCommit autocommit when message delivered
// not recommend use this function
func (k *Client) ListenWithAutoCommit(ctx context.Context, cMgs chan *Message) error {
	for _, r := range k.readers {
		go func(r *kafka.Reader) {
			for {
				m, err := r.ReadMessage(ctx) // is auto commit
				if err != nil && errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				if err != nil && errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					log.Print(err)
					continue
				}
				cMgs <- &Message{
					Offset:    m.Offset,
					Partition: m.Partition,
					Topic:     m.Topic,
					Body:      m.Value,
					Timestamp: m.Time.Unix(),
					Key:       string(m.Key),
					Commit:    func() {},
				}
			}
		}(r)
	}
	return nil
}
