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
	"log"
	"os"
	"sync"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	groupName := "testGroup"
	c, err := consumer.NewManualPullConsumer(
		consumer.WithGroupName(groupName),
		consumer.WithNameServer(primitive.NamesrvAddr{"127.0.0.1:9876"}),
	)
	if err != nil {
		log.Fatalf("init producer error: %v", err)
	}

	topic := "test"
	// get all message queue
	mqs := c.GetMessageQueues(context.Background(), topic)

	var wg sync.WaitGroup

	fn := func(mq *primitive.MessageQueue) {
		defer wg.Done()
		// get latest offset
		offset, err := c.CommittedOffset(groupName, mq)
		for {
			if err != nil {
				log.Fatalf("search latest offset error: %v", err)
			}
			// pull message
			ret, err := c.PullFromQueue(context.Background(), mq, offset, 1)
			if err != nil {
				log.Fatalf("pullFromQueue error: %v", err)
			}
			if ret.Status == primitive.PullFound {
				msgs := ret.GetMessageExts()
				for _, msg := range msgs {
					log.Printf("subscribe Msg: %v \n", msg)
					// commit offset
					if err = c.Seek(groupName, mq, msg.QueueOffset+1); err != nil {
						log.Fatalf("commit offset error: %v", err)
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
	os.Exit(0)
}
