package main

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/consumer"
	"github.com/apache/rocketmq-client-go/kernel"
	"os"
	"time"
)

func main() {
	c := consumer.NewPushConsumer("testGroup", consumer.ConsumerOption{
		ConsumerModel: consumer.Clustering,
		FromWhere:     consumer.ConsumeFromFirstOffset,
	})
	err := c.Subscribe("testTopic", consumer.MessageSelector{}, func(ctx *consumer.ConsumeMessageContext,
		msgs []*kernel.MessageExt) (consumer.ConsumeResult, error) {
		fmt.Println(msgs)
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Println(err.Error())
	}
	err = c.Start()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}
	time.Sleep(time.Hour)
}
