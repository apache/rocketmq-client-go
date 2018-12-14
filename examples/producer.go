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
	SendMessage()
}

func SendMessage() {
	producer := rocketmq.NewProducer(&rocketmq.ProducerConfig{GroupID: "testGroup", NameServer: "localhost:9876"})
	producer.Start()
	defer producer.Shutdown()

	fmt.Printf("Producer: %s started... \n", producer)
	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("Hello RocketMQ-%d", i)
		result := producer.SendMessageSync(&rocketmq.Message{Topic: "test", Body: msg})
		fmt.Println(fmt.Sprintf("send message: %s result: %s", msg, result))
	}
	time.Sleep(10 * time.Second)
	producer.Shutdown()
}
