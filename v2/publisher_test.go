package kafclient

import (
	"context"
	"log"
	"testing"
	"time"
)

func TestSendMessage(t *testing.T) {
	kclient := &Client{}
	kclient.SetAddrs([]string{"localhost:9092"})
	kclient.NewPublisher()
	for i := 0; i < 100; i++ {
		now := time.Now()
		err := kclient.Publish(context.TODO(), "topic-1", map[string]interface{}{
			"meta":  "tester2",
			"index": i,
			"topic": "topic-1",
		})
		log.Print(time.Since(now))
		if err != nil {
			log.Print("báo lỗi:", err)
		}
	}
	// for i := 0; i < 10; i++ {
	// 	err := kclient.Publish(context.TODO(), "topic-2", map[string]interface{}{
	// 		"meta":  "tester3",
	// 		"index": i,
	// 		"topic": "topic-2",
	// 	})
	// 	if err != nil {
	// 		log.Print(err)
	// 	}
	// }

}

func TestSendMessage2(t *testing.T) {
	kclient := &Client{}
	kclient.SetAddrs([]string{"b-1.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092", "b-2.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092"})
	kclient.NewPublisher()
	// go func() {
	// 	for i := 0; i < 100; i++ {
	// 		err := kclient.Publish(context.TODO(), "topic-4x", map[string]interface{}{
	// 			"meta":  "tester2",
	// 			"index": i,
	// 			"topic": "topic-2",
	// 		})
	// 		if err != nil {
	// 			log.Print(err)
	// 		}
	// 	}
	// }()
	// go func() {
	// 	for i := 0; i < 100; i++ {
	// 		err := kclient.Publish(context.TODO(), "topic-5x", map[string]interface{}{
	// 			"meta":  "tester2",
	// 			"index": i,
	// 			"topic": "topic-0",
	// 		})
	// 		if err != nil {
	// 			log.Print(err)
	// 		}
	// 	}
	// }()
	// go func() {
	// 	for i := 0; i < 100; i++ {
	// 		err := kclient.Publish(context.TODO(), "topic-3", map[string]interface{}{
	// 			"meta":  "tester2",
	// 			"index": i,
	// 			"topic": "topic-3",
	// 		})
	// 		if err != nil {
	// 			log.Print(err)
	// 		}
	// 	}
	// }()
	for i := 0; i < 300; i++ {
		err := kclient.Publish(context.TODO(), "topic-6x", map[string]interface{}{
			"meta":  "tester2",
			"index": i,
			"topic": "topic-1",
		})
		if err != nil {
			log.Print(err)
		}
	}
	// for i := 0; i < 100; i++ {
	// 	err := kclient.Publish(context.TODO(), "topic-2", map[string]interface{}{
	// 		"meta":  "tester3",
	// 		"index": i,
	// 		"topic": "topic-2",
	// 	})
	// 	if err != nil {
	// 		log.Print(err)
	// 	}
	// }

}
