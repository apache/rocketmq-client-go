package main

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

const (
	producerGroup = "please_rename_unique_group_name"
	consumerGroup = "please_rename_unique_group_name"
	topic         = "RequestTopic"
)

func main() {
	// create a producer to send reply message
	replyProducer, err := producer.NewDefaultProducer(
		producer.WithGroupName(producerGroup),
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}
	err = replyProducer.Start()
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}

	// create consumer
	c, err := consumer.NewPushConsumer(
		consumer.WithGroupName(consumerGroup),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset),
		consumer.WithPullInterval(0),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
	)

	err = c.Subscribe(topic, consumer.MessageSelector{
		Type: consumer.TAG, Expression: "*"}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {

		fmt.Printf("subscribe callback: %v \n", msgs)
		for _, msg := range msgs {
			fmt.Printf("handle message: %s", msg.String())
			// why sleep here ? because if not sleep, consumer reply message, but producer not register to broker, it will
			// make the broker can't find the reply channel.
			// only the go client has this problem.
			time.Sleep(time.Millisecond * 1000)
			fmt.Println("consumer sleep over, start reply")

			replyContent := []byte("reply message contents.")
			replyMessage, err := internal.CreateReplyMessage(msg, replyContent)
			if err != nil {
				fmt.Printf("create reply message err:%v\n", err)
				continue
			}

			replyTo := internal.GetReplyToClient(msg)
			replyResult, err := replyProducer.SendSync(context.Background(), replyMessage)
			if err != nil {
				fmt.Printf("send message error: %s\n", err)
				continue
			}
			fmt.Printf("reply to %s , %s \n", replyTo, replyResult.String())
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}
	err = c.Start()
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}
	fmt.Printf("Consumer Started.\n")

	time.Sleep(time.Hour)
	err = c.Shutdown()
	if err != nil {
		fmt.Printf("shutdown Consumer error: %s", err.Error())
	}
}
