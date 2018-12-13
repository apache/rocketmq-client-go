package main

import (
	"flag"
	"fmt"
	"github.com/apache/rocketmq-client-go/core"
)

var (
	namesrvAddrs string
	topic        string
	body         string
	groupID      string
	keys 	     string
)

func init() {
	flag.StringVar(&namesrvAddrs, "addr", "", "name server address")
	flag.StringVar(&topic, "t", "", "topic name")
	flag.StringVar(&groupID, "group", "", "producer group")
	flag.StringVar(&body, "body", "", "message body")
	flag.StringVar(&keys, "keys", "", "message keys")

}

func main()  {
	flag.Parse()
	if namesrvAddrs == "" {
		println("empty nameServer address")
		return
	}

	if topic == "" {
		println("empty topic")
		return
	}

	if body == "" {
		println("empty body")
		return
	}

	if groupID == "" {
		println("empty groupID")
		return
	}

	producer := rocketmq.NewProduer(&rocketmq.ProducerConfig{GroupID: groupID, NameServer: namesrvAddrs})
	producer.Start()
	defer producer.Shutdown()

	result := producer.SendMessageSync(&rocketmq.Message{Topic: topic, Body: body, Keys: keys})
	println(fmt.Sprintf("send message result: %s", result))
}