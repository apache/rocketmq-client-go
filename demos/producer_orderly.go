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

// Change to main if you want to run it directly
func main2() {
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
		ProducerModel: rocketmq.OrderlyProducer,
	}
	sendMessageOrderlyByShardingKey(pConfig)
}
func sendMessageOrderlyByShardingKey(config *rocketmq.ProducerConfig) {
	producer, err := rocketmq.NewProducer(config)
	if err != nil {
		fmt.Println("create Producer failed, error:", err)
		return
	}

	producer.Start()
	defer producer.Shutdown()
	for i := 0; i < 1000; i++ {
		msg := fmt.Sprintf("%s-%d", "Hello Lite Orderly Message", i)
		r, err := producer.SendMessageOrderlyByShardingKey(
			&rocketmq.Message{Topic: "YourOrderLyTopicXXXXXXXX", Body: msg}, "ShardingKey" /*orderID*/)
		if err != nil {
			println("Send Orderly Message Error:", err)
		}
		fmt.Printf("send orderly message result:%+v\n", r)
		time.Sleep(time.Duration(1) * time.Second)
	}

}
