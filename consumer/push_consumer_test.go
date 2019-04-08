package consumer

import (
	"github.com/apache/rocketmq-client-go/kernel"
	"testing"
	"time"
)

func TestNewPushConsumer(t *testing.T) {
	consumer := NewPushConsumer("testGroup", ConsumerOption{
		ConsumerModel: Clustering,
		FromWhere: ConsumeFromFirstOffset,
	})
	err := consumer.Subscribe("testTopic", MessageSelector{}, func(msg *kernel.Message) ConsumeResult {
		t.Log(string(msg.Body))
		return ConsumeSuccess
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	err = consumer.Start()
	if err != nil {
		t.Fatal(err.Error())
	}
	time.Sleep(time.Hour)
}
