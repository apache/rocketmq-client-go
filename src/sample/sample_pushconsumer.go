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

import "fmt"
import "../client"

func SampleConsumeMessage(msg client.MessageExt) (client.ConsumeStatus) {
	fmt.Println("ConsumeMessageInSample")
	fmt.Println("Message topic",client.GetMessageTopic(msg))
	fmt.Println("MessageId",client.GetMessageId(msg))
	return client.ConsumeSuccess
}

func SamplePushConsumeMessage() {
	fmt.Println("Start Send Message..")
	namesvr := "172.17.0.2:9876"
	topic := "T_TestTopic"
	expression := "*"
	//Create Producer
	consumer := client.CreatePushConsumer("testGroupId")
	fmt.Println("Create Push Consumer")
	client.SetPushConsumerNameServerAddress(consumer, namesvr)
	fmt.Println("Set Push Consumer Nameserver:", namesvr)

	client.Subscribe(consumer, topic, expression)
	fmt.Println("Set Push Consumer Subscribe,Topic:", topic," Exp:", expression)

	client.RegisterMessageCallback(consumer,SampleConsumeMessage)
	client.StartPushConsumer(consumer)
	fmt.Println("Start Push Consumer")
	fmt.Scan()
	select{}
	client.ShutdownPushConsumer(consumer)
	client.DestroyPushConsumer(consumer)
}
