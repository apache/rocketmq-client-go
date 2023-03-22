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
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/bilinxing/rocketmq-client-go/v2"

	"github.com/bilinxing/rocketmq-client-go/v2/rlog"

	"github.com/bilinxing/rocketmq-client-go/v2/consumer"
	"github.com/bilinxing/rocketmq-client-go/v2/primitive"
)

const (
	nameSrvAddr       = "http://127.0.0.1:9876"
	accessKey         = "rocketmq"
	secretKey         = "12345678"
	topic             = "test-topic"
	consumerGroupName = "testPullGroup"
	tag               = "testPull"
	namespace         = "ns"
)

var pullConsumer rocketmq.PullConsumer
var sleepTime = 1 * time.Second

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	rlog.SetLogLevel("info")
	var nameSrv, err = primitive.NewNamesrvAddr(nameSrvAddr)
	if err != nil {
		log.Fatalf("NewNamesrvAddr err: %v", err)
	}
	pullConsumer, err = rocketmq.NewPullConsumer(
		consumer.WithGroupName(consumerGroupName),
		consumer.WithNameServer(nameSrv),
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
		consumer.WithNamespace(namespace),
		consumer.WithMaxReconsumeTimes(2),
	)
	if err != nil {
		log.Fatalf("fail to new pullConsumer: %v", err)
	}
	selector := consumer.MessageSelector{
		Type:       consumer.TAG,
		Expression: tag,
	}
	err = pullConsumer.Subscribe(topic, selector)
	if err != nil {
		log.Fatalf("fail to Subscribe: %v", err)
	}
	err = pullConsumer.Start()
	if err != nil {
		log.Fatalf("fail to Start: %v", err)
	}

	for {
		poll()
	}
}

func poll() {
	cr, err := pullConsumer.Poll(context.TODO(), time.Second*5)
	if consumer.IsNoNewMsgError(err) {
		return
	}
	if err != nil {
		log.Printf("[poll error] err=%v", err)
		time.Sleep(sleepTime)
		return
	}
	// todo LOGIC CODE HERE
	log.Println("msgList: ", cr.GetMsgList())
	log.Println("messageQueue: ", cr.GetMQ())
	log.Println("processQueue: ", cr.GetPQ())
	// pullConsumer.ACK(context.TODO(), cr, consumer.ConsumeRetryLater)
	pullConsumer.ACK(context.TODO(), cr, consumer.ConsumeSuccess)
}
