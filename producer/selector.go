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
	Select(*primitive.Message, []*primitive.MessageQueue) *primitive.MessageQueue
}

// manualQueueSelector use the queue manually set in the provided Message's QueueID  field as the queue to send.
type manualQueueSelector struct{}

func NewManualQueueSelector() QueueSelector {
	return new(manualQueueSelector)
}

func (manualQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue) *primitive.MessageQueue {
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

func (r *randomQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue) *primitive.MessageQueue {
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

func (r *roundRobinQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue) *primitive.MessageQueue {
	t := message.Topic
	var idx *uint32

	r.Lock()
	idx, exist := r.indexer[t]
	if !exist {
		var v uint32 = 0
		idx = &v
		r.indexer[t] = idx
	}
	*idx++
	r.Unlock()

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
func (h *hashQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue) *primitive.MessageQueue {
	key := message.GetShardingKey()
	if len(key) == 0 {
		return h.random.Select(message, queues)
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

// brokerRoundRobinQueueSelector choose the queue by roundRobin.
type brokerRoundRobinQueueSelector struct {
	sync.Locker
	indexer map[string]*Indexer // 全局indexer
}

type Indexer struct {
	topicIndexer  *uint64
	brokerIndexer map[string]*brokerData
}

type brokerData struct {
	index   int
	count   int
	indexer *uint64
}

func NewBrokerRoundRobinQueueSelector() QueueSelector {
	s := &brokerRoundRobinQueueSelector{
		Locker:  new(sync.Mutex),
		indexer: make(map[string]*Indexer),
	}
	return s
}

func (r *brokerRoundRobinQueueSelector) Select(message *primitive.Message, queues []*primitive.MessageQueue) *primitive.MessageQueue {
	t := message.Topic

	r.Lock()
	brokerIdx, exist := r.indexer[t]
	if !exist {
		var v uint64 = 0
		idx := &v
		brokerIdx = &Indexer{
			topicIndexer:  idx,
			brokerIndexer: make(map[string]*brokerData),
		}
		r.indexer[t] = brokerIdx
	}

	*brokerIdx.topicIndexer++
	r.updateBrokerIndexer(brokerIdx, queues)
	qIndex := *brokerIdx.topicIndexer % uint64(len(brokerIdx.brokerIndexer))
	var bIndex uint64
	for _, v := range brokerIdx.brokerIndexer {
		if v.index == int(qIndex) {
			bIndex = *v.indexer % uint64(v.count)
			*v.indexer++
		}
	}
	r.Unlock()

	if qIndex == 0 {
		return queues[bIndex]
	}

	for qIndex > 0 {
		for _, v := range brokerIdx.brokerIndexer {
			if v.index == int(qIndex) {
				bIndex += uint64(v.count)
			}
		}
		qIndex--
	}

	return queues[bIndex]
}

func (r *brokerRoundRobinQueueSelector) updateBrokerIndexer(b *Indexer, queues []*primitive.MessageQueue) {
	// recalculated the count every time
	for k := range b.brokerIndexer {
		b.brokerIndexer[k].count = 0
	}

	globIndex := 0
	for i := range queues {
		v := queues[i]
		if v == nil {
			continue
		}
		if bv, ok := b.brokerIndexer[v.BrokerName]; !ok {
			var a uint64 = 0
			b.brokerIndexer[v.BrokerName] = &brokerData{
				index:   globIndex,
				indexer: &a,
				count:   1,
			}
			globIndex++
		} else {
			bv.count++
		}
	}

	// delete unused brokerIndexer
	for k := range b.brokerIndexer {
		if b.brokerIndexer[k].count == 0 {
			delete(b.brokerIndexer, k)
		}
	}
}
