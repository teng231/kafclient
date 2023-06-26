package kafclient

import (
	"context"
	"log"
	"testing"
)

func TestSendMessage(t *testing.T) {
	kclient := &Client{}
	kclient.SetAddrs([]string{"localhost:9092"})
	kclient.NewPublisher()
	for i := 0; i < 100; i++ {
		err := kclient.Publish(context.TODO(), "topic-1", map[string]interface{}{
			"meta":  "tester2",
			"index": i,
			"topic": "topic-1",
		})
		if err != nil {
			log.Print(err)
		}
	}
	for i := 0; i < 10; i++ {
		err := kclient.Publish(context.TODO(), "topic-2", map[string]interface{}{
			"meta":  "tester3",
			"index": i,
			"topic": "topic-2",
		})
		if err != nil {
			log.Print(err)
		}
	}

}
