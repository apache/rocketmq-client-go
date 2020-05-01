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

package admin

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func initAdmin(t *testing.T) Admin {
	var err error

	testAdmin, err := NewAdmin(WithResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	if err != nil {
		t.Fatal("init admin failed with %v.", err)
	}
	return testAdmin
}

func TestFetchConsumerOffset(t *testing.T) {
	testAdmin := initAdmin(t)

	ctx := context.Background()
	topic := "Test"
	group := "test_group"

	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "sandbox_boe1",
		QueueId:    0,
	}
	offset, err := testAdmin.FetchConsumerOffset(ctx, group, mq)
	assert.Nil(t, err)

	t.Log("get offset: %v", offset)
}

func TestFetchConsumerOffsets(t *testing.T) {
	testAdmin := initAdmin(t)

	ctx := context.Background()
	topic := "Test"
	group := "test_group"

	offsets, err := testAdmin.FetchConsumerOffsets(ctx, topic, group)
	assert.Nil(t, err)
	for _, offset := range offsets {
		t.Log("topic: %s brokerName: %s queueId: %d get Offset: %d", offset.Topic, offset.BrokerName, offset.QueueId, offset.Offset)
	}
}

func TestSearchOffset(t *testing.T) {
	testAdmin := initAdmin(t)

	ctx := context.Background()
	topic := "Test"
	tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2019-11-03 19:00:00", time.Local)
	assert.Nil(t, err)
	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "Default",
		QueueId:    1,
	}

	offset, err := testAdmin.SearchOffset(ctx, tm, mq)
	assert.Nil(t, err)
	t.Log("Offset val: %v", offset)
}

func TestResetConsumerOffset(t *testing.T) {
	TestFetchConsumerOffset(t)
	testAdmin := initAdmin(t)

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
	assert.Nil(t, err)
	t.Log("reset Offset success.")
}

func TestConcurrentSearchkey(t *testing.T) {
	for i := 0; i < 100; i++ {
		TestSearchKey(t)
	}
}

func TestSearchKey(t *testing.T) {
	// 确定没有问题, 看下 admin
	testAdmin := initAdmin(t)

	ctx := context.Background()
	topic := "Test"
	key := "6716311733805435655"
	maxNum := 32
	msgs, err := testAdmin.SearchKey(ctx, topic, key, maxNum)
	assert.Nil(t, err)
	for _, msg := range msgs {
		t.Log("msg: body:%v queue:%v\n", msg.StoreHost, *msg.Queue)
	}
}

func TestMinOffset(t *testing.T) {
	testAdmin := initAdmin(t)

	ctx := context.Background()
	topic := "Test"

	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "Default",
		QueueId:    0,
	}
	offset, err := testAdmin.MinOffset(ctx, mq)
	assert.Nil(t, err)

	t.Log("get topic min Offset: %v", offset)
}

func TestMaxOffset(t *testing.T) {
	testAdmin := initAdmin(t)

	ctx := context.Background()
	topic := "Test"

	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: "Default",
		QueueId:    0,
	}
	offset, err := testAdmin.MaxOffset(ctx, mq)
	assert.Nil(t, err)

	t.Log("get topic max Offset: %v", offset)
}

func TestMaxOffsets(t *testing.T) {
	testAdmin := initAdmin(t)

	ctx := context.Background()
	topic := "Test"

	offsets, err := testAdmin.MaxOffsets(ctx, topic)
	assert.Nil(t, err)
	for _, offset := range offsets {
		t.Log("get topic max Offset: %v", offset.String())
	}
}

func TestViewMessageByQueueOffset(t *testing.T) {
	testAdmin := initAdmin(t)

	ctx := context.Background()
	topic := "Test"

	offsets, err := testAdmin.MaxOffsets(ctx, topic)
	assert.Nil(t, err)
	for _, offset := range offsets {
		t.Log("get topic max Offset: %v", offset.String())
	}

	if len(offsets) > 0 {
		offset := offsets[0]

		msg, err := testAdmin.ViewMessageByQueueOffset(ctx, offset.MessageQueue, offset.Offset)
		if err != nil {
			t.Fatal("pull msgs get err: %v", err)
		}
		t.Log("get msgs: %v", msg)
	}

	time.Sleep(10 * time.Second)
}

func TestView(t *testing.T) {
	testAdmin := initAdmin(t)
	topic := "Test"
	_startTime := 1577203200000
	_endTime := 1577289600000

	startOffsets, err := testAdmin.SearchOffsets(context.Background(), topic, time.Unix(int64(_startTime/1000), 0))
	assert.Nil(t, err)
	endOffsets, err := testAdmin.SearchOffsets(context.Background(), topic, time.Unix(int64(_endTime/1000), 0))
	assert.Nil(t, err)

	for _, end := range endOffsets {
		if end.Offset > 0 {
			for _, startOffset := range startOffsets {
				if startOffset.BrokerName == end.BrokerName && startOffset.QueueId == end.QueueId {
					for offset := startOffset.Offset; offset <= end.Offset && offset < startOffset.Offset+5; offset++ {
						messageExts, err := testAdmin.ViewMessageByQueueOffset(context.Background(), end.MessageQueue, int64(offset))
						if err != nil {
							t.Log("view broker:%v,queue:%v,offset:%v message by offset error!%v", end.BrokerName, end.QueueId, end.Offset, err)
							continue
						}
						if messageExts != nil {
							t.Log("message ext: %v for queue: %v with offset: %d", messageExts, end.MessageQueue, messageExts.QueueOffset)
						}
					}
				}
			}
		}
	}
	time.Sleep(time.Second)
}

func TestGetConsumerConnectionList(t *testing.T) {
	testAdmin := initAdmin(t)

	ids, err := testAdmin.GetConsumerIdList(context.Background(), "test_group")
	assert.Nil(t, err)
	t.Log("consumer ids: %v", ids)
}

func TestAllocation(t *testing.T) {
	testAdmin := initAdmin(t)

	alloc, err := testAdmin.Allocation(context.Background(), "test_group")
	assert.Nil(t, err)
	t.Log("consumer alloc: %#v", alloc)
	time.Sleep(time.Second)
}

func TestGetConsumerRunningInfo(t *testing.T) {
	testAdmin := initAdmin(t)

	ids, err := testAdmin.GetConsumerRunningInfo(context.Background(), "test_group", "custom_client_id")
	assert.Nil(t, err)
	t.Log("consumer info: %#v", ids)
}

func TestMap(t *testing.T) {

	MQTable := map[string]internal.ProcessQueueInfo{
		"hahah": {
			Locked: true,
		},
	}
	data, err := json.Marshal(MQTable)
	assert.Nil(t, err)
	t.Log("data info: %v", string(data))

	b := map[string]internal.ProcessQueueInfo{}
	err = json.Unmarshal(data, &b)
	assert.Nil(t, err)
	t.Log("b: %v", b)
}

func TestComplex(t *testing.T) {

	MQTable := map[primitive.MessageQueue]internal.ProcessQueueInfo{
		{
			Topic:      "a",
			BrokerName: "B-a",
			QueueId:    1,
		}: {
			Locked: true,
		},
	}
	data, err := json.Marshal(MQTable)
	assert.Nil(t, err)
	t.Log("data info: %v", string(data))

	b := map[string]internal.ProcessQueueInfo{}
	err = json.Unmarshal(data, &b)
	assert.Nil(t, err)
	t.Log("b: %v", b)
}

func TestOffset(t *testing.T) {

	MQTable := map[consumer.MessageQueueKey]internal.ProcessQueueInfo{
		{
			Topic:      "a",
			BrokerName: "B-a",
			QueueId:    1,
		}: {
			Locked: true,
		},
	}
	data, err := json.Marshal(MQTable)
	assert.Nil(t, err)
	t.Log("data info: %v", string(data))

	//b := map[primitive.MessageQueue]internal.ProcessQueueInfo{}
	b := map[consumer.MessageQueueKey]internal.ProcessQueueInfo{}
	err = json.Unmarshal(data, &b)
	assert.Nil(t, err)
	t.Log("b: %v", b)
}
