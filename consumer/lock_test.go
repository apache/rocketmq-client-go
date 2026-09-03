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
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

// TestFetchLockReturnsSameLockForSameQueue is the deterministic proof of the bug: fetchLock must
// return the SAME lock instance for the same queue so that per-queue processing is serialized.
// With a value receiver on QueueLock (which embeds sync.Map), every call operates on a copy of the
// map, so LoadOrStore never persists into the shared map and a brand-new *sync.Mutex is returned
// each time.
//
//	buggy (value receiver)   -> l1 != l2 -> FAIL
//	fixed (pointer receiver) -> l1 == l2 -> PASS
func TestFetchLockReturnsSameLockForSameQueue(t *testing.T) {
	ql := newQueueLock()
	mq := primitive.MessageQueue{Topic: "t", BrokerName: "b", QueueId: 0}

	l1 := ql.fetchLock(mq)
	l2 := ql.fetchLock(mq)

	if l1 != l2 {
		t.Fatalf("fetchLock must return the same lock for the same queue, got two different instances (%p vs %p)", l1, l2)
	}
}

// TestFetchLockProvidesMutualExclusion demonstrates the real-world impact: consumeMessageOrderly
// relies on fetchLock(mq).Lock() to guarantee that only one goroutine processes a given queue at a
// time (FIFO). If every call returns a fresh mutex, there is no mutual exclusion and multiple
// goroutines enter the critical section concurrently.
//
//	buggy   -> maxConcurrent > 1 -> FAIL
//	fixed   -> maxConcurrent == 1 -> PASS
func TestFetchLockProvidesMutualExclusion(t *testing.T) {
	ql := newQueueLock()
	mq := primitive.MessageQueue{Topic: "t", BrokerName: "b", QueueId: 0}

	var stateMu sync.Mutex
	concurrent, maxConcurrent := 0, 0

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lock := ql.fetchLock(mq)
			lock.Lock()
			defer lock.Unlock()

			stateMu.Lock()
			concurrent++
			if concurrent > maxConcurrent {
				maxConcurrent = concurrent
			}
			stateMu.Unlock()

			time.Sleep(2 * time.Millisecond)

			stateMu.Lock()
			concurrent--
			stateMu.Unlock()
		}()
	}
	wg.Wait()

	if maxConcurrent != 1 {
		t.Fatalf("per-queue lock must serialize processing (maxConcurrent==1), got %d", maxConcurrent)
	}
}
