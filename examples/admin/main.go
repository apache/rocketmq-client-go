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
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/admin"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	TestFetchConsumerOffset()
}

func initAdmin() admin.Admin {
	var err error

	testAdmin, err := admin.NewAdmin(admin.WithResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	assert(err)
	return testAdmin
}

func TestFetchConsumerOffset() {
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"
	group := "test_group"

	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "sandbox_boe1",
		QueueId:    0,
	}
	offset, err := testAdmin.FetchConsumerOffset(ctx, group, mq)
	if err != nil {
		panic(err)
	}

	log.Printf("get offset: %v", offset)
}

func TestFetchConsumerOffsets() {
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"
	group := "test_group"

	offsets, err := testAdmin.FetchConsumerOffsets(ctx, topic, group)
	assert(err)
	for _, offset := range offsets {
		log.Printf("topic: %s brokerName: %s queueId: %d get Offset: %d", offset.Topic, offset.BrokerName, offset.QueueId, offset.Offset)
	}
}

func TestSearchOffset(t *testing.T) {
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"
	tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2019-11-03 19:00:00", time.Local)
	assert(err)
	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "Default",
		QueueId:    1,
	}

	offset, err := testAdmin.SearchOffset(ctx, tm, mq)
	assert(err)
	log.Printf("Offset val: %v", offset)
}

func TestResetConsumerOffset() {
	TestFetchConsumerOffset()
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"
	group := "test_group"
	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "Default",
		QueueId:    0,
	}
	offset := int64(272572362)
	err := testAdmin.ResetConsumerOffset(ctx, group, mq, offset)
	assert(err)
	log.Printf("reset Offset success.")
}

func TestConcurrentSearchkey(t *testing.T) {
	for i := 0; i < 100; i++ {
		TestSearchKey()
	}
}

func TestSearchKey() {
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"
	key := "6716311733805435655"
	maxNum := 32
	msgs, err := testAdmin.SearchKey(ctx, topic, key, maxNum)
	assert(err)
	for _, msg := range msgs {
		log.Printf("msg: body:%v queue:%v\n", msg.StoreHost, *msg.Queue)
	}
}

func TestMinOffset() {
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"

	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "Default",
		QueueId:    0,
	}
	offset, err := testAdmin.MinOffset(ctx, mq)
	assert(err)

	log.Printf("get topic min Offset: %v", offset)
}

func TestMaxOffset(t *testing.T) {
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"

	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "Default",
		QueueId:    0,
	}
	offset, err := testAdmin.MaxOffset(ctx, mq)
	assert(err)

	log.Printf("get topic max Offset: %v", offset)
}

func TestMaxOffsets() {
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"

	offsets, err := testAdmin.MaxOffsets(ctx, topic)
	assert(err)
	for _, offset := range offsets {
		log.Printf("get topic max Offset: %v", offset.String())
	}
}

func TestViewMessageByQueueOffset() {
	testAdmin := initAdmin()

	ctx := context.Background()
	topic := "Test"

	offsets, err := testAdmin.MaxOffsets(ctx, topic)
	assert(err)
	for _, offset := range offsets {
		log.Printf("get topic max Offset: %v", offset.String())
	}

	if len(offsets) > 0 {
		offset := offsets[0]

		msg, err := testAdmin.ViewMessageByQueueOffset(ctx, offset.MessageQueue, offset.Offset)
		if err != nil {
			log.Printf("pull msgs get err: %v", err)
		}
		log.Printf("get msgs: %v", msg)
	}
}

func TestView(t *testing.T) {
	testAdmin := initAdmin()
	topic := "Test"
	_startTime := 1577203200000
	_endTime := 1577289600000

	startOffsets, err := testAdmin.SearchOffsets(context.Background(), topic, time.Unix(int64(_startTime/1000), 0))
	assert(err)
	endOffsets, err := testAdmin.SearchOffsets(context.Background(), topic, time.Unix(int64(_endTime/1000), 0))
	assert(err)

	for _, end := range endOffsets {
		if end.Offset > 0 {
			for _, startOffset := range startOffsets {
				if startOffset.BrokerName == end.BrokerName && startOffset.QueueId == end.QueueId {
					for offset := startOffset.Offset; offset <= end.Offset && offset < startOffset.Offset+5; offset++ {
						messageExts, err := testAdmin.ViewMessageByQueueOffset(context.Background(), end.MessageQueue, int64(offset))
						if err != nil {
							log.Printf("view broker:%v,queue:%v,offset:%v message by offset error!%v", end.BrokerName, end.QueueId, end.Offset, err)
							continue
						}
						if messageExts != nil {
							log.Printf("message ext: %v for queue: %v with offset: %d", messageExts, end.MessageQueue, messageExts.QueueOffset)
						}
					}
				}
			}
		}
	}
}

func TestGetConsumerConnectionList() {
	testAdmin := initAdmin()

	ids, err := testAdmin.GetConsumerIdList(context.Background(), "test_group")
	assert(err)
	log.Printf("consumer ids: %v", ids)
}

func TestAllocation() {
	testAdmin := initAdmin()

	alloc, err := testAdmin.Allocation(context.Background(), "test_group")
	assert(err)
	log.Printf("consumer alloc: %#v", alloc)
}

func TestGetConsumerRunningInfo() {
	testAdmin := initAdmin()

	ids, err := testAdmin.GetConsumerRunningInfo(context.Background(), "test_group", "custom_client_id")
	assert(err)
	log.Printf("consumer info: %#v", ids)
}

func assert(err error) {
	if err != nil {
		panic(err)
	}
}
