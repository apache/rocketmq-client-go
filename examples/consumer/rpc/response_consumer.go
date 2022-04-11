package main

import (
	"context"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2/consumer"
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
		Type:       consumer.TAG,
		Expression: "*",
	}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		fmt.Printf("subscribe callback: %v \n", msgs)
		for _, msg := range msgs {
			fmt.Printf("handle message: %s", msg.String())
			replyTo := msg.GetProperty("REPLY_TO_CLIENT")
			replyContent := []byte("reply message contents.")
			// create reply message with given util, do not create reply message by yourself
			cluster := msg.GetProperty("CLUSTER")
			correlationId := msg.GetProperty("CORRELATION_ID")
			ttl := msg.GetProperty("TTL")
			var replyMessage *primitive.Message
			if cluster != "" {
				replyMessage = primitive.NewMessage(
					cluster+"_REPLY_TOPIC",
					replyContent,
				)
			} else {
				replyMessage = primitive.NewMessage(
					"",
					replyContent,
				)
			}
			replyMessage.WithProperty("MSG_TYPE", "reply")
			replyMessage.WithProperty("CORRELATION_ID", correlationId)
			replyMessage.WithProperty("REPLY_TO_CLIENT", replyTo)
			replyMessage.WithProperty("TTL", ttl)
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
}
