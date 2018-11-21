/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package main

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/core"
	"time"
)

func main() {
	PushConsumeMessage()
}

func PushConsumeMessage() {
	fmt.Println("Start Receiving Messages...")
	consumer, _ := rocketmq.NewPushConsumer(&rocketmq.ConsumerConfig{GroupID: "testGroupId", NameServer: "localhost:9876",
		ConsumerThreadCount: 2, MessageBatchMaxSize: 16})

	// MUST subscribe topic before consumer started.
	consumer.Subscribe("test", "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		fmt.Printf("A message received: \"%s\" \n", msg.Body)
		return rocketmq.ConsumeSuccess
	})

	consumer.Start()
	defer consumer.Shutdown()
	fmt.Printf("consumer: %s started...\n", consumer)
	time.Sleep(10 * time.Minute)
}
