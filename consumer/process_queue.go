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
	"container/list"
	"github.com/apache/rocketmq-client-go/kernel"
	"sync"
	"time"
)

const (
	_RebalanceLockMaxTime = 30 * time.Second
	_RebalanceInterval    = 20 * time.Second
	_PullMaxIdleTime      = 120 * time.Second
)

type ProcessQueue struct {
	mutex                      sync.RWMutex
	msgCache                   list.List // sorted
	cachedMsgCount             int
	cachedMsgSize              int64
	consumeLock                sync.Mutex
	consumingMsgOrderlyTreeMap sync.Map
	tryUnlockTimes             int64
	queueOffsetMax             int64
	dropped                    bool
	lastPullTime               time.Time
	lastConsumeTime            time.Time
	locked                     bool
	lastLockTime               time.Time
	consuming                  bool
	msgAccCnt                  int64
	once                       sync.Once
}

func (pq *ProcessQueue) isPullExpired() bool {
	return false
}

func (pq *ProcessQueue) getMaxSpan() int {
	return pq.msgCache.Len()
}

func (pq *ProcessQueue) putMessage(messages []*kernel.MessageExt) {
	pq.once.Do(func() {
		pq.msgCache.Init()
	})
	localList := list.New()
	for idx := range messages {
		localList.PushBack(messages[idx])
	}
	pq.mutex.Lock()
	pq.msgCache.PushBackList(localList)
	pq.mutex.Unlock()
}

func (pq *ProcessQueue) removeMessage(number int) int {
	i := 0
	pq.mutex.Lock()
	for ; i < number && pq.msgCache.Len() > 0; i++ {
		pq.msgCache.Remove(pq.msgCache.Front())
	}
	pq.mutex.Unlock()
	return i
}

func (pq *ProcessQueue) takeMessages(number int) []*kernel.MessageExt {
	for pq.msgCache.Len() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	result := make([]*kernel.MessageExt, number)
	i := 0
	pq.mutex.Lock()
	for ; i < number; i++ {
		e := pq.msgCache.Front()
		if e == nil {
			break
		}
		result[i] = e.Value.(*kernel.MessageExt)
		pq.msgCache.Remove(e)
	}
	pq.mutex.Unlock()
	return result[:i]
}
