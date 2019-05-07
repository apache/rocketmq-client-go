package rocketmq

import (
	"testing"
)

func TestProducer_Send(t *testing.T) {
	producer := NewProducer(ProducerConfig{
		GroupName: "testGroup",
	})
	producer.Start()
}
