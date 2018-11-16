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

func SampleSendMessage() {
	fmt.Println("Start Send Message..")
	namesvr := "172.17.0.2:9876"
	topic := "T_TestTopic"
	keys := "testKeys"
	body := "testBody"
	//Create Producer
	producer := client.CreateProducer("testGroupId")
	fmt.Println("Create Producer")
	client.SetProducerNameServerAddress(producer, namesvr)
	fmt.Println("Set Producer Nameserver:", namesvr)
	client.StartProducer(producer)
	fmt.Println("Start Producer")

	for i := 1; i <= 10; i++ {
		//Create Message
		msg := client.CreateMessage(topic)
		fmt.Println("Create Message, Topic:", topic)
		client.SetMessageKeys(msg, keys)
		fmt.Println("Set Message Keys:", keys)
		client.SetMessageBody(msg, body)
		fmt.Println("Set Message body:", body)

		sendresult := client.SendMessageSync(producer, msg)
		fmt.Println("Send Message OK")
		fmt.Println("SendStatus:", sendresult.Status)
		fmt.Println("SendResult:", sendresult)
		client.DestroyMessage(msg)
	}
	client.ShutdownProducer(producer)
	client.DestroyProducer(producer)
}
