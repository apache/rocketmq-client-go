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

package rocketmq

import "C"
import (
	"strconv"
	"sync"
	"unsafe"
)

var selectors = selectorHolder{selectors: map[int]*messageQueueSelectorWrapper{}}

//export queueSelectorCallback
func queueSelectorCallback(size int, selectorKey unsafe.Pointer) int {
	s, ok := selectors.getAndDelete(*(*int)(selectorKey))
	if !ok {
		panic("BUG: not register the selector with key:" + strconv.Itoa(*(*int)(selectorKey)))
	}
	return s.Select(size)
}

type messageQueueSelectorWrapper struct {
	selector MessageQueueSelector

	m   *Message
	arg interface{}
}

func (w *messageQueueSelectorWrapper) Select(size int) int {
	return w.selector.Select(size, w.m, w.arg)
}

// MessageQueueSelector select one message queue
type MessageQueueSelector interface {
	Select(size int, m *Message, arg interface{}) int
}

type selectorHolder struct {
	sync.Mutex

	selectors map[int]*messageQueueSelectorWrapper
	key       int
}

func (s *selectorHolder) put(selector *messageQueueSelectorWrapper) (key int) {
	s.Lock()
	key = s.key
	s.selectors[key] = selector
	s.key++
	s.Unlock()
	return
}

func (s *selectorHolder) getAndDelete(key int) (*messageQueueSelectorWrapper, bool) {
	s.Lock()
	selector, ok := s.selectors[key]
	if ok {
		delete(s.selectors, key)
	}
	s.Unlock()

	return selector, ok
}
