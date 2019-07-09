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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-client-go/primitive"
)

type QueueSelector interface {
	Select(*primitive.Message, int) int
}

// manualQueueSelector use the queue manually set in the provided Message's QueueID  field as the queue to send.
type manualQueueSelector struct{}

func NewManualQueueSelector() QueueSelector {
	return new(manualQueueSelector)
}

func (manualQueueSelector) Select(message *primitive.Message, queues int) int {
	return message.QueueID
}

// randomQueueSelector choose a random queue each time.
type randomQueueSelector struct {
	rander *rand.Rand
}

func NewRandomQueueSelector() QueueSelector {
	s := new(randomQueueSelector)
	s.rander = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	return s
}

func (r randomQueueSelector) Select(message *primitive.Message, queues int) int {
	return r.rander.Intn(queues)
}

// roundRobinQueueSelector choose the queue by roundRobin.
type roundRobinQueueSelector struct {
	sync.Locker
	indexer map[string]*int32
}

func NewRoundRobinQueueSelector() QueueSelector {
	s := &roundRobinQueueSelector{
		Locker:  new(sync.Mutex),
		indexer: map[string]*int32{},
	}
	return s
}

func (r *roundRobinQueueSelector) Select(message *primitive.Message, queues int) int {
	t := message.Topic
	if _, exist := r.indexer[t]; !exist {
		r.Lock()
		if _, exist := r.indexer[t]; !exist {
			var v = int32(0)
			r.indexer[t] = &v
		}
		r.Unlock()
	}
	index := r.indexer[t]

	i := atomic.AddInt32(index, 1)
	if i < 0 {
		i = -i
		atomic.StoreInt32(index, 0)
	}
	return int(i) % queues
}
