package kafclient

import "testing"

func TestListTopic(t *testing.T) {
	e := Client{
		addrs: []string{"b-1.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092", "b-2.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092"},
	}
	e.ListTopics()
}

func TestListTopic2(t *testing.T) {
	e := Client{
		addrs: []string{"b-1.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092", "b-2.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092"},
	}
	e.ListTopics2()
}
func TestCreateTopic(t *testing.T) {
	e := Client{
		addrs: []string{"b-1.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092", "b-2.urboxhnstaging.k17vzh.c3.kafka.ap-southeast-1.amazonaws.com:9092"},
	}
	e.CreateTopic("1688275678", -1)
}
