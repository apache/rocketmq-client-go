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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockMessageQueueSelector struct {
	arg  interface{}
	m    *Message
	size int

	selectRet int
}

func (m *mockMessageQueueSelector) Select(size int, msg *Message, arg interface{}) int {
	m.arg, m.m, m.size = arg, msg, size
	return m.selectRet
}

func TestWrapper(t *testing.T) {
	s := &mockMessageQueueSelector{selectRet: 2}
	w := &messageQueueSelectorWrapper{selector: s, m: &Message{}, arg: 3}

	assert.Equal(t, 2, w.Select(4))
	assert.Equal(t, w.m, s.m)
	v, ok := s.arg.(int)
	assert.True(t, ok)
	assert.Equal(t, 3, v)
}

func TestSelectorHolder(t *testing.T) {
	s := &messageQueueSelectorWrapper{}

	key := selectors.put(s)
	assert.Equal(t, 0, key)

	key = selectors.put(s)
	assert.Equal(t, 1, key)

	assert.Equal(t, 2, len(selectors.selectors))

	ss, ok := selectors.getAndDelete(0)
	assert.Equal(t, s, ss)
	assert.True(t, ok)

	ss, ok = selectors.getAndDelete(1)
	assert.Equal(t, s, ss)
	assert.True(t, ok)

	assert.Equal(t, 0, len(selectors.selectors))
}
