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

// Change to main if you want to run it directly
func main0() {
	pConfig := &rocketmq.ProducerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    "GID_XXXXXXXXXXXX",
			NameServer: "http://XXXXXXXXXXXXXXXXXX:80",
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: "Your Access Key",
				SecretKey: "Your Secret Key",
				Channel:   "ALIYUN/OtherChannel",
			},
		},
		//Set to Common Producer as default.
		ProducerModel: rocketmq.CommonProducer,
	}
	sendMessage(pConfig)
}
func sendMessage(config *rocketmq.ProducerConfig) {
	producer, err := rocketmq.NewProducer(config)

	if err != nil {
		fmt.Println("create common producer failed, error:", err)
		return
	}

	err = producer.Start()
	if err != nil {
		fmt.Println("start common producer error", err)
		return
	}
	defer producer.Shutdown()

	fmt.Printf("Common producer: %s started... \n", producer)
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("%s-%d", "Hello,Common MQ Message-", i)
		result, err := producer.SendMessageSync(&rocketmq.Message{Topic: "YourTopicXXXXXXXX", Body: msg})
		if err != nil {
			fmt.Println("Error:", err)
		}
		fmt.Printf("send message: %s result: %s\n", msg, result)
	}
	fmt.Println("shutdown common producer.")
}
