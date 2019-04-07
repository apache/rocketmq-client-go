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
	msgCache                   sync.Map // sorted
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
}

func (pq *ProcessQueue) isPullExpired() bool {
	return false
}

func (pq *ProcessQueue) getMaxSpan() int {
	return 0
}

func (pq *ProcessQueue) putMessage(messages []*kernel.MessageExt) bool {
	return true
}

func (pq *ProcessQueue) removeMessage(number int) int64 {
	return 0
}
