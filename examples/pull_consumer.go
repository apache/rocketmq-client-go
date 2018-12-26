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
	"time"

	"github.com/apache/rocketmq-client-go/core"
)

func ConsumeWithPull(config *rocketmq.PullConsumerConfig, topic string) {

	consumer, err := rocketmq.NewPullConsumer(config)
	if err != nil {
		fmt.Printf("new pull consumer error:%s\n", err)
		return
	}

	err = consumer.Start()
	if err != nil {
		fmt.Printf("start consumer error:%s\n", err)
		return
	}
	defer consumer.Shutdown()

	mqs := consumer.FetchSubscriptionMessageQueues(topic)
	fmt.Printf("fetch subscription mqs:%+v\n", mqs)

	total, offsets, now := 0, map[int]int64{}, time.Now()

PULL:
	for {
		for _, mq := range mqs {
			pr := consumer.Pull(mq, "*", offsets[mq.ID], 32)
			total += len(pr.Messages)
			fmt.Printf("pull %s, result:%+v\n", mq.String(), pr)

			switch pr.Status {
			case rocketmq.PullNoNewMsg:
				break PULL
			case rocketmq.PullFound:
				fallthrough
			case rocketmq.PullNoMatchedMsg:
				fallthrough
			case rocketmq.PullOffsetIllegal:
				offsets[mq.ID] = pr.NextBeginOffset
			case rocketmq.PullBrokerTimeout:
				fmt.Println("broker timeout occur")
			}
		}
	}

	var timePerMessage time.Duration
	if total > 0 {
		timePerMessage = time.Since(now) / time.Duration(total)
	}
	fmt.Printf("total message:%d, per message time:%d\n", total, timePerMessage)
}
