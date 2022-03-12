package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func main() {
	p, _ := rocketmq.NewProducer(
		producer.WithGroupName("please_rename_unique_group_name"),
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})),
		producer.WithRetry(2),
	)
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	topic := "RequestTopic"
	ttl := 300 * time.Second
	msg := &primitive.Message{
		Topic: topic,
		Body:  []byte("Hello RPC RocketMQ Go Client!"),
	}

	now := time.Now()
	responseMsg, err := p.Request(context.Background(), ttl, msg)
	if err != nil {
		fmt.Printf("Request message error: %s\n", err)
		return
	}
	fmt.Printf("Requst to %s cost:%d ms responseMsg:%s\n", topic, time.Since(now).Milliseconds(), responseMsg.String())

	err = p.Shutdown()
	if err != nil {
		fmt.Printf("shutdown producer error: %s", err.Error())
	}
}
