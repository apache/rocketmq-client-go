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
	"time"

	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/go-redis/redis/v8"
)

const (
	namesrvAddr       = "http://127.0.0.1:9876"
	accessKey         = "rocketmq"
	secretKey         = "12345678"
	topic             = "test-topic"
	consumerGroupName = "testGroup"
	tag               = "testPull"
	namespace         = "ns"
)

var ctx = context.Background()
var client = redis.NewClient(&redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})

func main() {
	var namesrv, err = primitive.NewNamesrvAddr(namesrvAddr)
	if err != nil {
		rlog.Fatal(fmt.Sprintf("NewNamesrvAddr err: %s", err), nil)
	}
	c, err := consumer.NewPullConsumer(
		consumer.WithGroupName(consumerGroupName),
		consumer.WithNameServer(namesrv),
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
		consumer.WithNamespace(namespace),
	)
	if err != nil {
		rlog.Fatal(fmt.Sprintf("fail to new pullConsumer: %s", err), nil)
	}
	err = c.Start()
	if err != nil {
		rlog.Fatal(fmt.Sprintf("fail to new pullConsumer: %s", err), nil)
	}

	nameSrvAddr := []string{namesrvAddr}
	admin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
		admin.WithNamespace(namespace),
	)
	if err != nil {
		rlog.Fatal(fmt.Sprintf("fail to new admin: %s", err), nil)
	}

	messageQueues, err := admin.FetchPublishMessageQueues(context.Background(), topic)
	if err != nil {
		rlog.Fatal(fmt.Sprintf("fetch messahe queue error: %s", err), nil)
	}
	var timeSleepSeconds = 1 * time.Second
	selector := consumer.MessageSelector{
		Type:       consumer.TAG,
		Expression: tag,
	}
	for _, queue := range messageQueues {
		offset := getOffset(client, consumerGroupName, topic, queue.QueueId)
		if offset < 0 { // default consume from offset 0
			offset = 0
		}
		go func(queue *primitive.MessageQueue) {
			for {
				resp, err := c.PullFrom(ctx, selector, queue, offset, 1) // pull one message per time for make sure easily ACK
				if err != nil {
					fmt.Printf("[pull error] topic=%s, queue id=%d, broker=%s, offset=%d", topic, queue.QueueId, queue.BrokerName, offset)
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
							ackOffset(client, consumerGroupName, topic, queue.QueueId, resp.NextBeginOffset)

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

		}(queue)
	}
	// make current thread hold to see pull result. TODO should update here
	time.Sleep(10000 * time.Second)
}

func ackOffset(redis *redis.Client, consumerGroupName string, topic string, queueId int, consumedOffset int64) {
	var key = fmt.Sprintf("rmq-%s-%s-%d", consumerGroupName, topic, queueId)
	err := redis.Set(ctx, key, consumedOffset, 0).Err()
	if err != nil {
		fmt.Printf("set redis 失败, key:%s, %+v\n", key, err)
	}
}

func getOffset(redisCli *redis.Client, consumerGroupName string, topic string, queueId int) int64 {
	var key = fmt.Sprintf("rmq-%s-%s-%d", consumerGroupName, topic, queueId)
	offset, err := redisCli.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0
	} else if err != nil {
		fmt.Printf("get redis 失败, key:%s, %+v\n", key, err)
		return 0
	} else {
		return offset
	}
}
