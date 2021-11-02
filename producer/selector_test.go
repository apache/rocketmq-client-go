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

package producer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func TestRoundRobin(t *testing.T) {
	queues := make([]*primitive.MessageQueue, 10)
	for i := 0; i < 10; i++ {
		queues = append(queues, &primitive.MessageQueue{
			QueueId: i,
		})
	}
	s := NewRoundRobinQueueSelector()

	m := &primitive.Message{
		Topic: "test",
	}
	mrr := &primitive.Message{
		Topic: "rr",
	}
	for i := 0; i < 100; i++ {
		q := s.Select(m, queues)
		expected := (i + 1) % len(queues)
		assert.Equal(t, queues[expected], q, "i: %d", i)

		qrr := s.Select(mrr, queues)
		expected = (i + 1) % len(queues)
		assert.Equal(t, queues[expected], qrr, "i: %d", i)
	}
}

func TestHashQueueSelector(t *testing.T) {
	queues := make([]*primitive.MessageQueue, 10)
	for i := 0; i < 10; i++ {
		queues = append(queues, &primitive.MessageQueue{
			QueueId: i,
		})
	}

	s := NewHashQueueSelector()

	m1 := &primitive.Message{
		Topic: "test",
		Body:  []byte("one message"),
	}
	m1.WithShardingKey("same_key")
	q1 := s.Select(m1, queues)

	m2 := &primitive.Message{
		Topic: "test",
		Body:  []byte("another message"),
	}
	m2.WithShardingKey("same_key")
	q2 := s.Select(m2, queues)
	assert.Equal(t, *q1, *q2)
}

func TestBrokerRoundRobinQueueSelector(t *testing.T) {
	t.Run("singleBrokerSingleQueueNum", func(t *testing.T) {
		singleBrokerSingleQueueNumQueues := make([]*primitive.MessageQueue, 0, 1)
		singleBrokerSingleQueueNumQueues = append(singleBrokerSingleQueueNumQueues, &primitive.MessageQueue{
			BrokerName: "broker-a",
			QueueId:    0,
		})
		s := NewBrokerRoundRobinQueueSelector()
		m := &primitive.Message{
			Topic: "test",
			Body:  []byte("one message"),
		}
		q1 := s.Select(m, singleBrokerSingleQueueNumQueues)
		q2 := s.Select(m, singleBrokerSingleQueueNumQueues)
		assert.Equal(t, *q1, *q2)
	})

	t.Run("singleBrokerMultiQueueNum", func(t *testing.T) {
		singleBrokerMultiQueueNumQueues := make([]*primitive.MessageQueue, 0, 8)
		for i := 0; i < 8; i++ {
			singleBrokerMultiQueueNumQueues = append(singleBrokerMultiQueueNumQueues, &primitive.MessageQueue{
				BrokerName: "broker-a",
				QueueId:    i,
			})
		}

		s := NewBrokerRoundRobinQueueSelector()
		m := &primitive.Message{
			Topic: "test",
			Body:  []byte("one message"),
		}
		for i := 0; i < 100; i++ {
			q := s.Select(m, singleBrokerMultiQueueNumQueues)
			expected := (i) % len(singleBrokerMultiQueueNumQueues)
			assert.Equal(t, singleBrokerMultiQueueNumQueues[expected], q, "i: %d", i)
		}
	})

	t.Run("multiBrokerSingleQueueNum", func(t *testing.T) {
		multiBrokerSingleQueueNumQueues := make([]*primitive.MessageQueue, 0, 2)
		multiBrokerSingleQueueNumQueues = append(multiBrokerSingleQueueNumQueues, &primitive.MessageQueue{
			BrokerName: "broker-a",
			QueueId:    0,
		})
		multiBrokerSingleQueueNumQueues = append(multiBrokerSingleQueueNumQueues, &primitive.MessageQueue{
			BrokerName: "broker-b",
			QueueId:    0,
		})

		s := NewBrokerRoundRobinQueueSelector()
		m := &primitive.Message{
			Topic: "test",
			Body:  []byte("one message"),
		}
		var preq *primitive.MessageQueue
		for i := 0; i < 100; i++ {
			q := s.Select(m, multiBrokerSingleQueueNumQueues)
			if preq != nil {
				assert.NotEqual(t, q.BrokerName, preq.BrokerName)
				assert.Equal(t, q.QueueId, preq.QueueId)
			}
			preq = q
		}
	})

	t.Run("multiBrokerMultiQueueNum", func(t *testing.T) {
		multiBrokerMultiQueueNumQueues := make([]*primitive.MessageQueue, 0, 8)
		for i := 0; i < 8; i++ {
			var brokerName string
			if i < 4 {
				brokerName = "broker-a"
			} else {
				brokerName = "broker-b"
			}

			multiBrokerMultiQueueNumQueues = append(multiBrokerMultiQueueNumQueues, &primitive.MessageQueue{
				BrokerName: brokerName,
				QueueId:    i % 4,
			})
		}

		s := NewBrokerRoundRobinQueueSelector()
		m := &primitive.Message{
			Topic: "test",
			Body:  []byte("one message"),
		}
		var expected int
		var preq *primitive.MessageQueue
		for i := 0; i < 100; i++ {
			q := s.Select(m, multiBrokerMultiQueueNumQueues)

			if i%2 == 0 && i > 0 {
				expected++
			}
			assert.Equal(t, q.QueueId, expected%4)

			if preq != nil {
				assert.NotEqual(t, preq.BrokerName, q.BrokerName)
			}
			preq = q
		}
	})
}
