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
	"github.com/apache/rocketmq-client-go/core"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
)

var (
	rmq     = kingpin.New("rocketmq", "RocketMQ cmd tools")
	namesrv = rmq.Flag("namesrv", "NameServer address.").Default("localhost:9876").Short('n').String()
	topic   = rmq.Flag("topic", "topic name.").Short('t').Required().String()
	gid     = rmq.Flag("groupId", "group Id").Short('g').Default("testGroup").String()
	amount  = rmq.Flag("amount", "how many message to produce or consume").Default("64").Short('a').Int()

	produce     = rmq.Command("produce", "send messages to RocketMQ")
	body        = produce.Flag("body", "message body").Short('b').Required().String()
	workerCount = produce.Flag("workerCount", "works of send message with orderly").Default("1").Short('w').Int()
	orderly     = produce.Flag("orderly", "send msg orderly").Short('o').Bool()

	consume = rmq.Command("consume", "consumes message from RocketMQ")
)

func main() {
	switch kingpin.MustParse(rmq.Parse(os.Args[1:])) {
	case produce.FullCommand():
		pConfig := &rocketmq.ProducerConfig{ClientConfig: rocketmq.ClientConfig{
			GroupID:    *gid,
			NameServer: *namesrv,
			LogC: &rocketmq.LogConfig{
				Path:     "example",
				FileSize: 64 * 1 << 10,
				FileNum:  1,
				Level:    rocketmq.LogLevelDebug,
			},
		}}
		if *orderly {
			sendMessageOrderly(pConfig)
		} else {
			sendMessage(pConfig)
		}
	case consume.FullCommand():
		cConfig := &rocketmq.PushConsumerConfig{ClientConfig: rocketmq.ClientConfig{
			GroupID:    *gid,
			NameServer: *namesrv,
			LogC: &rocketmq.LogConfig{
				Path:     "example",
				FileSize: 64 * 1 << 10,
				FileNum:  1,
				Level:    rocketmq.LogLevelInfo,
			},
		}}

		ConsumeWithPush(cConfig)
	}
}
