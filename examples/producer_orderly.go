/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/rocketmq-client-go/core"
)

type queueSelectorByOrderID struct{}

func (s queueSelectorByOrderID) Select(size int, m *rocketmq.Message, arg interface{}) int {
	return arg.(int) % size
}

type worker struct {
	p            rocketmq.Producer
	leftMsgCount int64
}

func (w *worker) run() {
	selector := queueSelectorByOrderID{}
	for atomic.AddInt64(&w.leftMsgCount, -1) >= 0 {
		r, err := w.p.SendMessageOrderly(
			&rocketmq.Message{Topic: *topic, Body: *body}, selector, 7 /*orderID*/, 3,
		)
		if err != nil {
			println("Send Orderly Error:", err)
		}
		fmt.Printf("send orderly result:%+v\n", r)
	}
}

func sendMessageOrderly(config *rocketmq.ProducerConfig) {
	producer, err := rocketmq.NewProducer(config)
	if err != nil {
		fmt.Println("create Producer failed, error:", err)
		return
	}

	producer.Start()
	defer producer.Shutdown()

	wg := sync.WaitGroup{}
	wg.Add(*workerCount)

	workers := make([]worker, *workerCount)
	for i := range workers {
		workers[i].p = producer
		workers[i].leftMsgCount = (int64)(*amount)
	}

	for i := range workers {
		go func(w *worker) { w.run(); wg.Done() }(&workers[i])
	}

	wg.Wait()
}
