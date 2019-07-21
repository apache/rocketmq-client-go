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

package consumer

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	gods_util "github.com/emirpasic/gods/utils"
)

const (
	_RebalanceLockMaxTime = 30 * time.Second
	_RebalanceInterval    = 20 * time.Second
	_PullMaxIdleTime      = 120 * time.Second
)

type processQueue struct {
	msgCache                   *treemap.Map
	mutex                      sync.RWMutex
	cachedMsgCount             int64
	cachedMsgSize              int64
	consumeLock                sync.Mutex
	consumingMsgOrderlyTreeMap *treemap.Map
	tryUnlockTimes             int64
	queueOffsetMax             int64
	dropped                    bool
	lastPullTime               time.Time
	lastConsumeTime            time.Time
	locked                     bool
	lastLockTime               time.Time
	consuming                  bool
	msgAccCnt                  int64
	lockConsume                sync.Mutex
	msgCh                      chan []*primitive.MessageExt
	order                      bool
}

func newProcessQueue(order bool) *processQueue {
	consumingMsgOrderlyTreeMap := treemap.NewWith(gods_util.Int64Comparator)

	pq := &processQueue{
		msgCache:                   treemap.NewWith(utils.Int64Comparator),
		lastPullTime:               time.Now(),
		lastConsumeTime:            time.Now(),
		lastLockTime:               time.Now(),
		msgCh:                      make(chan []*primitive.MessageExt, 32),
		consumingMsgOrderlyTreeMap: consumingMsgOrderlyTreeMap,
		order:                      order,
	}
	return pq
}

func (pq *processQueue) putMessage(messages ...*primitive.MessageExt) {
	if messages == nil || len(messages) == 0 {
		return
	}
	pq.mutex.Lock()
	if !pq.order {
		pq.msgCh <- messages
	}
	validMessageCount := 0
	for idx := range messages {
		msg := messages[idx]
		_, found := pq.msgCache.Get(msg.QueueOffset)
		if found {
			continue
		}
		pq.msgCache.Put(msg.QueueOffset, msg)
		validMessageCount++
		pq.queueOffsetMax = msg.QueueOffset
		atomic.AddInt64(&pq.cachedMsgSize, int64(len(msg.Body)))
	}
	pq.mutex.Unlock()

	atomic.AddInt64(&pq.cachedMsgCount, int64(validMessageCount))

	if pq.msgCache.Size() > 0 && !pq.consuming {
		pq.consuming = true
	}

	msg := messages[len(messages)-1]
	maxOffset, err := strconv.ParseInt(msg.Properties[primitive.PropertyMaxOffset], 10, 64)
	if err != nil {
		acc := maxOffset - msg.QueueOffset
		if acc > 0 {
			pq.msgAccCnt = acc
		}
	}
}

func (pq *processQueue) makeMessageToCosumeAgain(messages ...*primitive.MessageExt) {
	pq.mutex.Lock()
	for _, msg := range messages {
		pq.consumingMsgOrderlyTreeMap.Remove(msg.QueueOffset)
		pq.msgCache.Put(msg.QueueOffset, msg)
	}

	pq.mutex.Unlock()
}

func (pq *processQueue) removeMessage(messages ...*primitive.MessageExt) int64 {
	result := int64(-1)
	pq.mutex.Lock()
	pq.lastConsumeTime = time.Now()
	if !pq.msgCache.Empty() {
		result = pq.queueOffsetMax + 1
		removedCount := 0
		for idx := range messages {
			msg := messages[idx]
			_, found := pq.msgCache.Get(msg.QueueOffset)
			if !found {
				continue
			}
			pq.msgCache.Remove(msg.QueueOffset)
			removedCount++
			atomic.AddInt64(&pq.cachedMsgSize, int64(-len(msg.Body)))
		}
		atomic.AddInt64(&pq.cachedMsgCount, int64(-removedCount))
	}
	if !pq.msgCache.Empty() {
		first, _ := pq.msgCache.Min()
		result = first.(int64)
	}
	pq.mutex.Unlock()
	return result
}

func (pq *processQueue) isLockExpired() bool {
	return time.Now().Sub(pq.lastLockTime) > _RebalanceLockMaxTime
}

func (pq *processQueue) isPullExpired() bool {
	return time.Now().Sub(pq.lastPullTime) > _PullMaxIdleTime
}

func (pq *processQueue) cleanExpiredMsg(consumer defaultConsumer) {
	if consumer.option.ConsumeOrderly {
		return
	}
	var loop = 16
	if pq.msgCache.Size() < 16 {
		loop = pq.msgCache.Size()
	}

	for i := 0; i < loop; i++ {
		pq.mutex.RLock()
		if pq.msgCache.Empty() {
			pq.mutex.RLock()
			return
		}
		_, firstValue := pq.msgCache.Min()
		msg := firstValue.(*primitive.MessageExt)
		startTime := msg.Properties[primitive.PropertyConsumeStartTime]
		if startTime != "" {
			st, err := strconv.ParseInt(startTime, 10, 64)
			if err != nil {
				rlog.Warnf("parse message start consume time error: %s, origin str is: %s", startTime)
				continue
			}
			if time.Now().Unix()-st <= int64(consumer.option.ConsumeTimeout) {
				pq.mutex.RLock()
				return
			}
		}
		pq.mutex.RLock()

		err := consumer.sendBack(msg, 3)
		if err != nil {
			rlog.Errorf("send message back to broker error: %s when clean expired messages", err.Error())
			continue
		}
		pq.removeMessage(msg)
	}
}

func (pq *processQueue) getMaxSpan() int {
	pq.mutex.RLock()
	defer pq.mutex.RUnlock()
	if pq.msgCache.Size() == 0 {
		return 0
	}
	firstKey, _ := pq.msgCache.Min()
	lastKey, _ := pq.msgCache.Max()
	return int(lastKey.(int64) - firstKey.(int64))
}

func (pq *processQueue) getMessages() []*primitive.MessageExt {
	return <-pq.msgCh
}

func (pq *processQueue) takeMessages(number int) []*primitive.MessageExt {
	for pq.msgCache.Empty() {
		time.Sleep(10 * time.Millisecond)
	}
	result := make([]*primitive.MessageExt, number)
	i := 0
	pq.mutex.Lock()
	for ; i < number; i++ {
		k, v := pq.msgCache.Min()
		if v == nil {
			break
		}
		result[i] = v.(*primitive.MessageExt)
		pq.consumingMsgOrderlyTreeMap.Put(k, v)
		pq.msgCache.Remove(k)
	}
	pq.mutex.Unlock()
	return result[:i]
}

func (pq *processQueue) Min() int64 {
	if pq.msgCache.Empty() {
		return -1
	}
	k, _ := pq.msgCache.Min()
	if k != nil {
		return k.(int64)
	}
	return -1
}

func (pq *processQueue) Max() int64 {
	if pq.msgCache.Empty() {
		return -1
	}
	k, _ := pq.msgCache.Max()
	if k != nil {
		return k.(int64)
	}
	return -1
}

func (pq *processQueue) clear() {
	pq.mutex.Lock()
	pq.msgCache.Clear()
	pq.cachedMsgCount = 0
	pq.cachedMsgSize = 0
	pq.queueOffsetMax = 0
}

func (pq *processQueue) commit() int64 {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	var offset int64
	iter, _ := pq.consumingMsgOrderlyTreeMap.Max()
	if iter != nil {
		offset = iter.(int64)
	}
	pq.cachedMsgCount -= int64(pq.consumingMsgOrderlyTreeMap.Size())
	pq.consumingMsgOrderlyTreeMap.Each(func(key interface{}, value interface{}) {
		msg := value.(*primitive.MessageExt)
		pq.cachedMsgSize -= int64(len(msg.Body))
	})
	pq.consumingMsgOrderlyTreeMap.Clear()
	return offset + 1
}
