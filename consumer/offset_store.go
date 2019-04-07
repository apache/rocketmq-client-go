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

import "github.com/apache/rocketmq-client-go/kernel"

type readType int

const (
	_ReadFromMemory readType = iota
	_ReadFromStore
	_ReadMemoryThenStore
)

type OffsetStore interface {
	load()
	persist(mqs []*kernel.MessageQueue)
	remove(mq *kernel.MessageQueue)
	read(mq *kernel.MessageQueue, t readType) int64
	update(mq *kernel.MessageQueue, offset int64, increaseOnly bool)
}

type localFileOffsetStore struct {
}

func (local *localFileOffsetStore) load() {}
func (local *localFileOffsetStore) persist(mqs []*kernel.MessageQueue) {}
func (local *localFileOffsetStore) remove(mq *kernel.MessageQueue) {}
func (local *localFileOffsetStore) read(mq *kernel.MessageQueue, t readType) int64 {return 0}
func (local *localFileOffsetStore) update(mq *kernel.MessageQueue, offset int64, increaseOnly bool) {}

type remoteBrokerOffsetStore struct {

}

func (remote *remoteBrokerOffsetStore) load() {}
func (remote *remoteBrokerOffsetStore) persist(mqs []*kernel.MessageQueue) {}
func (remote *remoteBrokerOffsetStore) remove(mq *kernel.MessageQueue) {}
func (remote *remoteBrokerOffsetStore) read(mq *kernel.MessageQueue, t readType) int64 {return 0}
func (remote *remoteBrokerOffsetStore) update(mq *kernel.MessageQueue, offset int64, increaseOnly bool) {}
