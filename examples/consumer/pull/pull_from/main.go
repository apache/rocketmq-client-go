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
	"log"
	"time"

	"github.com/bilinxing/rocketmq-client-go/v2"

	"github.com/bilinxing/rocketmq-client-go/v2/admin"
	"github.com/bilinxing/rocketmq-client-go/v2/consumer"
	"github.com/bilinxing/rocketmq-client-go/v2/primitive"
	"github.com/go-redis/redis/v8"
)

const (
	nameSrvAddr       = "http://127.0.0.1:9876"
	accessKey         = "rocketmq"
	secretKey         = "12345678"
	topic             = "test-topic"
	consumerGroupName = "testPullFromGroup"
	tag               = "testPullFrom"
	namespace         = "ns"
)

var ctx = context.Background()
var client = redis.NewClient(&redis.Options{
	Addr:     "127.0.0.1:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
})

func main() {
	var nameSrv, err = primitive.NewNamesrvAddr(nameSrvAddr)
	if err != nil {
		log.Fatalf("NewNamesrvAddr err: %v", err)
	}
	pullConsumer, err := rocketmq.NewPullConsumer(
		consumer.WithGroupName(consumerGroupName),
		consumer.WithNameServer(nameSrv),
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
		consumer.WithNamespace(namespace),
	)
	if err != nil {
		log.Fatalf("fail to new pullConsumer: %v", err)
	}

	selector := consumer.MessageSelector{
		Type:       consumer.TAG,
		Expression: tag,
	}
	err = pullConsumer.Subscribe(topic, selector)
	if err != nil {
		log.Fatalf("Subscribe error: %s\n", err)
	}
	err = pullConsumer.Start()
	if err != nil {
		log.Fatalf("fail to new pullConsumer: %v", err)
	}

	nameSrvAddr := []string{nameSrvAddr}
	mqAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			AccessKey: accessKey,
			SecretKey: secretKey,
		}),
		admin.WithNamespace(namespace),
	)
	if err != nil {
		log.Fatalf("fail to new admin: %v", err)
	}

	messageQueues, err := mqAdmin.FetchPublishMessageQueues(ctx, topic)
	if err != nil {
		log.Fatalf("fetch messahe queue error: %s\n", err)
	}
	var sleepTime = 1 * time.Second

	for _, queue := range messageQueues {
		offset := getOffset(client, consumerGroupName, topic, queue.QueueId)
		if offset < 0 { // default consume from offset 0
			offset = 0
		}
		go func(queue *primitive.MessageQueue) {
			for {
				resp, err := pullConsumer.PullFrom(ctx, queue, offset, 1) // pull one message per time for make sure easily ACK
				if err != nil {
					log.Printf("[pull error] topic=%s, queue id=%d, broker=%s, offset=%d\n", topic, queue.QueueId, queue.BrokerName, offset)
					time.Sleep(sleepTime)
					continue
				}
				switch resp.Status {
				case primitive.PullFound:
					log.Printf("[pull message successfully] queue id = %d, nextOffset: %d \n", queue.QueueId, resp.NextBeginOffset)
					for _, msg := range resp.GetMessageExts() {
						// todo LOGIC CODE HERE

						log.Println(msg.GetKeys(), msg.MsgId, string(msg.Body))

						// save offset to redis
						err = ackOffset(client, consumerGroupName, topic, queue.QueueId, resp.NextBeginOffset)
						if err != nil {
							//todo ack error logic
						}

						// set offset for next pull
						offset = resp.NextBeginOffset
					}
				case primitive.PullNoNewMsg, primitive.PullNoMsgMatched:
					log.Printf("[no pull message] topic=%s, queue id=%d, broker=%s, offset=%d,  next = %d\n", queue.Topic, queue.QueueId, queue.BrokerName, offset, resp.NextBeginOffset)
					time.Sleep(sleepTime)
					offset = resp.NextBeginOffset
					continue
				case primitive.PullBrokerTimeout:
					log.Printf("[pull broker timeout] topic=%s, queue id=%d, broker=%s, offset=%d,  next = %d\n", queue.Topic, queue.QueueId, queue.BrokerName, offset, resp.NextBeginOffset)

					time.Sleep(sleepTime)
					continue
				case primitive.PullOffsetIllegal:
					log.Printf("[pull offset illegal] topic=%s, queue id=%d, broker=%s, offset=%d,  next = %d\n", queue.Topic, queue.QueueId, queue.BrokerName, offset, resp.NextBeginOffset)
					offset = resp.NextBeginOffset
					continue
				default:
					log.Printf("[pull error] topic=%s, queue id=%d, broker=%s, offset=%d,  next = %d\n", queue.Topic, queue.QueueId, queue.BrokerName, offset, resp.NextBeginOffset)
				}
			}
		}(queue)
	}
	// make current thread hold to see pull result. TODO should update here
	time.Sleep(10000 * time.Second)
}

func ackOffset(redis *redis.Client, consumerGroupName string, topic string, queueId int, consumedOffset int64) error {
	var key = fmt.Sprintf("rmq-%s-%s-%d", consumerGroupName, topic, queueId)
	err := redis.Set(ctx, key, consumedOffset, 0).Err()
	if err != nil {
		log.Printf("set redis error, key:%s, %v\n", key, err)
		return err
	}
	return nil
}

func getOffset(redisCli *redis.Client, consumerGroupName string, topic string, queueId int) int64 {
	var key = fmt.Sprintf("rmq-%s-%s-%d", consumerGroupName, topic, queueId)
	offset, err := redisCli.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0
	} else if err != nil {
		log.Printf("get redis error, key:%s, %v\n", key, err)
		//todo Your own logic. like get from db.
		return 0
	} else {
		return offset
	}
}
