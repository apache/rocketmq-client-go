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
)

func sendMessage(config *rocketmq.ProducerConfig) {
	producer, err := rocketmq.NewProducer(config)

	if err != nil {
		fmt.Println("create Producer failed, error:", err)
		return
	}

	err = producer.Start()
	if err != nil {
		fmt.Println("start producer error", err)
		return
	}
	defer producer.Shutdown()

	fmt.Printf("Producer: %s started... \n", producer)
	for i := 0; i < *amount; i++ {
		msg := fmt.Sprintf("%s-%d", *body, i)
		result, err := producer.SendMessageSync(&rocketmq.Message{Topic: *topic, Body: msg})
		if err != nil {
			fmt.Println("Error:", err)
		}
		fmt.Printf("send message: %s result: %s\n", msg, result)
	}
	fmt.Println("shutdown producer.")
}
