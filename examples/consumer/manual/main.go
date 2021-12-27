/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

func main() {
	groupName := "testGroup"
	c, err := consumer.NewManualPullConsumer(
		consumer.WithGroupName(groupName),
		consumer.WithNameServer(primitive.NamesrvAddr{"127.0.0.1:9876"}),
	)
	if err != nil {
		rlog.Fatal(fmt.Sprintf("init producer error: %v", err), nil)
	}

	topic := "test"
	// get all message queue
	mqs, err := c.GetMessageQueues(context.Background(), topic)
	if err != nil {
		rlog.Fatal(fmt.Sprintf("get message queue error: %v", err), nil)
	}
	var wg sync.WaitGroup

	fn := func(mq *primitive.MessageQueue) {
		defer wg.Done()
		// get latest offset
		offset, err := c.CommittedOffset(context.Background(), groupName, mq)
		if err != nil {
			rlog.Fatal(fmt.Sprintf("search consumer offset error: %v", err), nil)
		}
		for {
			// pull message
			ret, err := c.PullFromQueue(context.Background(), groupName, mq, offset, 1)
			if err != nil {
				rlog.Fatal(fmt.Sprintf("pullFromQueue error: %v", err), nil)
			}
			if ret.Status == primitive.PullFound {
				msgs := ret.GetMessageExts()
				for _, msg := range msgs {
					fmt.Printf("subscribe Msg: %v \n", msg)
					// commit offset
					if err = c.Seek(context.Background(), groupName, mq, msg.QueueOffset+1); err != nil {
						rlog.Fatal(fmt.Sprintf("commit offset error: %v", err), nil)
					}
					offset++
				}
			} else {
				break
			}
		}
	}

	for _, mq := range mqs {
		wg.Add(1)
		go fn(mq)
	}
	wg.Wait()
	c.Shutdown()
}
