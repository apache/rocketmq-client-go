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
	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-redis/redis/v8"
	"time"
)

var ctx = context.Background()
var client = redis.NewClient(&redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})

func main() {
	var namesrvAddr = "127.0.0.1:9876"
	var namesrv, _ = primitive.NewNamesrvAddr(namesrvAddr)
	var consumerGroupName = "testGroup"
	c, err := consumer.NewPullConsumer(
		consumer.WithGroupName(consumerGroupName),
		consumer.WithNameServer(namesrv),
	)
	if err != nil {
		rlog.Fatal(fmt.Sprintf("fail to new pullConsumer: %s", err), nil)
	}
	err = c.Start()
	if err != nil {
		rlog.Fatal(fmt.Sprintf("fail to new pullConsumer: %s", err), nil)
	}

	var topic = "1-test-topic"

	nameSrvAddr := []string{namesrvAddr}
	admin, _ := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)))
	messageQueues, err := admin.FetchPublishMessageQueues(context.Background(), topic)
	if err != nil {
		rlog.Fatal(fmt.Sprintf("fetch messahe queue error: %s", err), nil)
	}
	var timeSleepSeconds = 1 * time.Second
	for _, queue1 := range messageQueues {
		offset := getOffset(client, consumerGroupName, queue1.QueueId)
		if offset < 0 { // default consume from offset 0
			offset = 0
		}
		queue := queue1
		go func() {
			for {
				resp, err := c.PullFrom(ctx, queue, offset, 1) // pull one message per time for make sure easily ACK
				if err != nil {
					fmt.Printf("[pull error] topic=%s, queue id=%d, broker=%s, offset=%d", topic, queue1.QueueId, queue.BrokerName, offset)
					time.Sleep(timeSleepSeconds)
					continue
				}

				switch resp.Status {
				case primitive.PullFound:
					{
						fmt.Printf("pull message success. queue id = %d, nextOffset: %d \n", queue.QueueId, resp.NextBeginOffset)
						for _, msg := range resp.GetMessageExts() {
							// todo LOGIC CODE HERE

							fmt.Println(string(msg.Body))

							// save offset to redis
							ackOffset(client, consumerGroupName, queue.QueueId, resp.NextBeginOffset)

							// set offset for next pull
							offset = resp.NextBeginOffset
						}
					}
				case primitive.PullNoNewMsg, primitive.PullNoMsgMatched:
					{
						fmt.Printf("[no pull message] topic=%s, queue id=%d, broker=%s, offset=%d,  next = %d", queue.Topic, queue.QueueId, queue.BrokerName, offset, resp.NextBeginOffset)
						time.Sleep(timeSleepSeconds)
						offset = resp.NextBeginOffset
						continue
					}

				case primitive.PullBrokerTimeout:
					{
						fmt.Printf("[pull broker timeout] topic=%s, queue id=%d, broker=%s, offset=%d,  next = %d", queue.Topic, queue.QueueId, queue.BrokerName, offset, resp.NextBeginOffset)

						time.Sleep(timeSleepSeconds)
						continue
					}

				case primitive.PullOffsetIllegal:
					{
						fmt.Printf("[pull offset illegal] topic=%s, queue id=%d, broker=%s, offset=%d,  next = %d", queue.Topic, queue.QueueId, queue.BrokerName, offset, resp.NextBeginOffset)
						offset = resp.NextBeginOffset
						continue
					}

				default:
					fmt.Printf("[pull error] topic=%s, queue id=%d, broker=%s, offset=%d,  next = %d", queue.Topic, queue.QueueId, queue.BrokerName, offset, resp.NextBeginOffset)
				}
			}

		}()
	}
	// make current thread hold to see pull result. TODO should update here
	time.Sleep(10000 * time.Second)
}

func ackOffset(redis *redis.Client, consumerGroupName string, queueId int, consumedOffset int64) {
	var key = fmt.Sprintf("rmq-%s-%d", consumerGroupName, queueId)
	err := redis.Set(ctx, key, consumedOffset, 0).Err()
	if err != nil {
		fmt.Printf("set redis 失败 %+v\n", err)
	}
}

func getOffset(redis *redis.Client, consumerGroupName string, queueId int) int64 {
	var key = fmt.Sprintf("rmq-%s-%d", consumerGroupName, queueId)
	offset, err := redis.Get(ctx, key).Int64()
	if err != nil {
		fmt.Printf("set redis 失败 %+v\n", err)
	}
	return offset
}
