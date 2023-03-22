/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/bilinxing/rocketmq-client-go/v2"
	"github.com/bilinxing/rocketmq-client-go/v2/primitive"
	"github.com/bilinxing/rocketmq-client-go/v2/producer"
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
	ttl := 5 * time.Second
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
	fmt.Printf("Requst to %s cost:%d ms responseMsg:%s\n", topic, time.Since(now)/time.Millisecond, responseMsg.String())

	err = p.Shutdown()
	if err != nil {
		fmt.Printf("shutdown producer error: %s", err.Error())
	}
}
