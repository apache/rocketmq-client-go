package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

var (
	topic         string
	endpoint      string
	accessKey     string
	secretKey     string
	messageCount  int
	delayInterval int
)

func init() {
	flag.StringVar(&topic, "topic", "benchmark-queue-1", "topic name")
	flag.StringVar(&endpoint, "endpoint", "127.0.0.1:9876", "endpoint")
	flag.StringVar(&accessKey, "access_key", "******", "access key")
	flag.StringVar(&secretKey, "secret_key", "******", "secret key")
	flag.IntVar(&messageCount, "count", 10, "message count")
	flag.IntVar(&delayInterval, "delay", 0, "delay interval unit: sec")
}

// Package main implements a simple producer to send message.
func main() {
	flag.Parse()

	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{endpoint})),
		producer.WithRetry(2),
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}

	for i := 0; i < messageCount; i++ {
		msg := &primitive.Message{
			Topic: topic,
			Body: []byte("Hello RocketMQ Go Client! " + strconv.Itoa(i) +
				" timestamp:" + strconv.FormatInt(time.Now().Unix(), 10)),
		}
		if delayInterval > 0 {
			msg.WithDelayTimestamp(time.Now().Add(time.Duration(delayInterval) * time.Second))
		}
		res, err := p.SendSync(context.Background(), msg)

		if err != nil {
			fmt.Printf("index: %v, send message error: %s\n", i+1, err)
			break
		} else {
			fmt.Printf("index: %v, send message success: result=%s\n", i+1, res.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	err = p.Shutdown()
	if err != nil {
		fmt.Printf("shutdown producer error: %s", err.Error())
	}
}
