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
	"sync/atomic"
)

func ConsumeWithPush(config *rocketmq.PushConsumerConfig) {

	consumer, err := rocketmq.NewPushConsumer(config)
	if err != nil {
		println("create Consumer failed, error:", err)
		return
	}

	ch := make(chan interface{})
	var count = (int64)(*amount)
	// MUST subscribe topic before consumer started.
	consumer.Subscribe(*topic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		fmt.Printf("A message received: \"%s\" \n", msg.Body)
		if atomic.AddInt64(&count, -1) <= 0 {
			ch <- "quit"
		}
		return rocketmq.ConsumeSuccess
	})

	err = consumer.Start()
	if err != nil {
		println("consumer start failed,", err)
		return
	}

	fmt.Printf("consumer: %s started...\n", consumer)
	<-ch
	err = consumer.Shutdown()
	if err != nil {
		println("consumer shutdown failed")
		return
	}
	println("consumer has shutdown.")
}
