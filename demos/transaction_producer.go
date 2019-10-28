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
func main4() {
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
		//Set to Trans Producer as default.
		ProducerModel: rocketmq.TransProducer,
	}
	sendTransactionMessage(pConfig)
}

type myTransactionLocalListener struct {
}

func (l *myTransactionLocalListener) Execute(m *rocketmq.Message, arg interface{}) rocketmq.TransactionStatus {
	return rocketmq.UnknownTransaction
}
func (l *myTransactionLocalListener) Check(m *rocketmq.MessageExt, arg interface{}) rocketmq.TransactionStatus {
	return rocketmq.CommitTransaction
}
func sendTransactionMessage(config *rocketmq.ProducerConfig) {
	listener := &myTransactionLocalListener{}
	producer, err := rocketmq.NewTransactionProducer(config, listener, nil)

	if err != nil {
		fmt.Println("create Transaction producer failed, error:", err)
		return
	}

	err = producer.Start()
	if err != nil {
		fmt.Println("start Transaction producer error", err)
		return
	}
	defer producer.Shutdown()

	fmt.Printf("Transaction producer: %s started... \n", producer)
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("%s-%d", "Hello,Transaction MQ Message-", i)
		result, err := producer.SendMessageTransaction(&rocketmq.Message{Topic: "YourTopicXXXXXXXX", Body: msg}, nil)
		if err != nil {
			fmt.Println("Error:", err)
		}
		fmt.Printf("send message: %s result: %s\n", msg, result)
	}
	time.Sleep(time.Duration(1) * time.Minute)
	fmt.Println("shutdown Transaction producer.")
}
