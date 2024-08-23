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
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

type QueueSelector interface {
	Select(msg *primitive.Message, mqs []*primitive.MessageQueue, lastBrokerName string) *primitive.MessageQueue
}

// manualQueueSelector use the queue manually set in the provided Message's QueueID  field as the queue to send.
type manualQueueSelector struct{}

func NewManualQueueSelector() QueueSelector {
	return new(manualQueueSelector)
}

func (manualQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue, lastBrokerName string) *primitive.MessageQueue {
	return message.Queue
}

// randomQueueSelector choose a random queue each time.
type randomQueueSelector struct {
	mux    sync.Mutex
	rander *rand.Rand
}

func NewRandomQueueSelector() QueueSelector {
	s := new(randomQueueSelector)
	s.rander = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	return s
}

func (r *randomQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue, lastBrokerName string) *primitive.MessageQueue {
	r.mux.Lock()
	i := r.rander.Intn(len(queues))
	r.mux.Unlock()
	return queues[i]
}

// roundRobinQueueSelector choose the queue by roundRobin.
type roundRobinQueueSelector struct {
	sync.Locker
	indexer map[string]*uint32
}

func NewRoundRobinQueueSelector() QueueSelector {
	s := &roundRobinQueueSelector{
		Locker:  new(sync.Mutex),
		indexer: map[string]*uint32{},
	}
	return s
}

func (r *roundRobinQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue, lastBrokerName string) *primitive.MessageQueue {
	t := message.Topic

	r.Lock()
	defer r.Unlock()
	if lastBrokerName != "" {
		for i := 0; i < len(queues); i++ {
			idx, exist := r.indexer[t]
			if !exist {
				var v uint32 = 0
				idx = &v
				r.indexer[t] = idx
			}
			*idx++
			qIndex := *idx % uint32(len(queues))
			if queues[qIndex].BrokerName != lastBrokerName {
				return queues[qIndex]
			}
		}
	}
	return r.selectOneMessageQueue(t, queues)
}

func (r *roundRobinQueueSelector) selectOneMessageQueue(t string, queues []*primitive.MessageQueue) *primitive.MessageQueue {
	var idx *uint32

	idx, exist := r.indexer[t]
	if !exist {
		var v uint32 = 0
		idx = &v
		r.indexer[t] = idx
	}
	*idx++

	qIndex := *idx % uint32(len(queues))
	return queues[qIndex]
}

type hashQueueSelector struct {
	random QueueSelector
}

func NewHashQueueSelector() QueueSelector {
	return &hashQueueSelector{
		random: NewRandomQueueSelector(),
	}
}

// hashQueueSelector choose the queue by hash if message having sharding key, otherwise choose queue by random instead.
func (h *hashQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue, lastBrokerName string) *primitive.MessageQueue {
	key := message.GetShardingKey()
	if len(key) == 0 {
		return h.random.Select(message, queues, lastBrokerName)
	}

	hasher := fnv.New32a()
	_, err := hasher.Write([]byte(key))
	if err != nil {
		return nil
	}
	queueId := int(hasher.Sum32()) % len(queues)
	if queueId < 0 {
		queueId = -queueId
	}
	return queues[queueId]
}
