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
	"encoding/json"
	"github.com/apache/rocketmq-client-go/kernel"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/apache/rocketmq-client-go/utils"
	"os"
	"path/filepath"
	"sync"
)

type readType int

const (
	_ReadFromMemory readType = iota
	_ReadFromStore
	_ReadMemoryThenStore
)

var (
	_LocalOffsetStorePath = os.Getenv("rocketmq.client.localOffsetStoreDir")
)

func init() {
	if _LocalOffsetStorePath == "" {
		_LocalOffsetStorePath = filepath.Join(os.Getenv("user.home"), ".rocketmq_client_go")
	}
}

type OffsetStore interface {
	load()
	persist(mqs []*kernel.MessageQueue)
	remove(mq *kernel.MessageQueue)
	read(mq *kernel.MessageQueue, t readType) int64
	update(mq *kernel.MessageQueue, offset int64, increaseOnly bool)
}

type localFileOffsetStore struct {
	group       string
	path        string
	OffsetTable map[string]map[int]*queueOffset `json:"OffsetTable"`
	// mutex for offset file
	mutex sync.Mutex
}

type queueOffset struct {
	QueueID int    `json:"queueId"`
	Broker  string `json:"brokerName"`
	Offset  int64  `json:"offset"`
}

func NewLocalFileOffsetStore(clientID, group string) *localFileOffsetStore {
	store := &localFileOffsetStore{
		group: group,
		path:  filepath.Join(_LocalOffsetStorePath, clientID, group, "offset.json"),
	}
	store.load()
	return store
}

func (local *localFileOffsetStore) load() {
	local.mutex.Lock()
	defer local.mutex.Unlock()
	data, err := utils.FileReadAll(local.path)
	if err != nil {
		data, err = utils.FileReadAll(filepath.Join(local.path, ".bak"))
	}
	if err != nil {
		rlog.Debugf("load local offset: %s error: %s", local.path, err.Error())
		return
	}
	err = json.Unmarshal(data, local)
	if err != nil {
		rlog.Debugf("unmarshal local offset: %s error: %s", local.path, err.Error())
		return
	}
}

func (local *localFileOffsetStore) read(mq *kernel.MessageQueue, t readType) int64 {
	if t == _ReadFromMemory || t == _ReadMemoryThenStore {
		off := local.readFromMemory(mq)
		if off >= 0 || (off == -1 && t == _ReadFromMemory) {
			return off
		}
	}
	local.load()
	return local.readFromMemory(mq)
}

func (local *localFileOffsetStore) readFromMemory(mq *kernel.MessageQueue) int64 {
	localOffset, exist := local.OffsetTable[mq.Topic]
	if !exist {
		return -1
	}
	off, exist := localOffset[mq.QueueId]
	if !exist {
		return -1
	}

	return off.Offset
}

func (local *localFileOffsetStore) update(mq *kernel.MessageQueue, offset int64, increaseOnly bool) {
	localOffset, exist := local.OffsetTable[mq.Topic]
	if !exist {
		localOffset = make(map[int]*queueOffset)
		local.OffsetTable[mq.Topic] = localOffset
	}
	q, exist := localOffset[mq.QueueId]
	if !exist {
		q = &queueOffset{
			QueueID: mq.QueueId,
			Broker:  mq.BrokerName,
		}
		localOffset[mq.QueueId] = q
	}
	if increaseOnly {
		if q.Offset < offset {
			q.Offset = offset
		}
	} else {
		q.Offset = offset
	}
}

func (local *localFileOffsetStore) persist(mqs []*kernel.MessageQueue) {
	if len(mqs) == 0 {
		return
	}
	s := new(struct {
		OffsetTable map[string]map[int]*queueOffset `json:"offsetTable"`
	})
	table := make(map[string]map[int]*queueOffset)
	for idx := range mqs {
		mq := mqs[idx]
		offsets, exist := local.OffsetTable[mq.Topic]
		if !exist {
			continue
		}
		off, exist := offsets[mq.QueueId]
		if !exist {
			continue
		}

		offsets, exist = table[mq.Topic]
		if !exist {
			offsets = make(map[int]*queueOffset)
		}
		offsets[off.QueueID] = off
	}
	data, _ := json.Marshal(s)
	utils.WriteToFile(local.path, data)
}

func (local *localFileOffsetStore) remove(mq *kernel.MessageQueue) {
	// unsupported
}

type remoteBrokerOffsetStore struct {
}

func (remote *remoteBrokerOffsetStore) load() {

}

func (remote *remoteBrokerOffsetStore) persist(mqs []*kernel.MessageQueue) {

}

func (remote *remoteBrokerOffsetStore) remove(mq *kernel.MessageQueue) {

}

func (remote *remoteBrokerOffsetStore) read(mq *kernel.MessageQueue, t readType) int64 {
	return 0
}

func (remote *remoteBrokerOffsetStore) update(mq *kernel.MessageQueue, offset int64, increaseOnly bool) {

}
