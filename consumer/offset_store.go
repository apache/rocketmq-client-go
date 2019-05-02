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
	"fmt"
	"github.com/apache/rocketmq-client-go/kernel"
	"github.com/apache/rocketmq-client-go/remote"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/apache/rocketmq-client-go/utils"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
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

func NewLocalFileOffsetStore(clientID, group string) OffsetStore {
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
		off := readFromMemory(local.OffsetTable, mq)
		if off >= 0 || (off == -1 && t == _ReadFromMemory) {
			return off
		}
	}
	local.load()
	return readFromMemory(local.OffsetTable, mq)
}

func (local *localFileOffsetStore) update(mq *kernel.MessageQueue, offset int64, increaseOnly bool) {
	rlog.Debugf("update offset: %s to %d", mq, offset)
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
	utils.CheckError(fmt.Sprintf("persist offset to %s", local.path), utils.WriteToFile(local.path, data))
}

func (local *localFileOffsetStore) remove(mq *kernel.MessageQueue) {
	// unsupported
}

type remoteBrokerOffsetStore struct {
	group       string
	OffsetTable map[string]map[int]*queueOffset `json:"OffsetTable"`
	mutex       sync.RWMutex
}

func NewRemoteOffsetStore(group string) OffsetStore {
	return &remoteBrokerOffsetStore{
		group:       group,
		OffsetTable: make(map[string]map[int]*queueOffset),
	}
}

func (remote *remoteBrokerOffsetStore) load() {
	// unsupported
}

func (remote *remoteBrokerOffsetStore) persist(mqs []*kernel.MessageQueue) {
	remote.mutex.Lock()
	defer remote.mutex.Unlock()
	if len(mqs) == 0 {
		return
	}
	for idx := range mqs {
		mq := mqs[idx]
		offsets, exist := remote.OffsetTable[mq.Topic]
		if !exist {
			continue
		}
		off, exist := offsets[mq.QueueId]
		if !exist {
			continue
		}

		err := updateConsumeOffsetToBroker(remote.group, mq.Topic, off)
		if err != nil {
			rlog.Warnf("update offset to broker error: %s, group: %s, queue: %s, offset: %d",
				err.Error(), remote.group, mq.String(), off.Offset)
		} else {
			rlog.Debugf("update offset to broker success, group: %s, topic: %s, queue: %v", remote.group, mq.Topic, off)
		}
	}
}

func (remote *remoteBrokerOffsetStore) remove(mq *kernel.MessageQueue) {
	remote.mutex.Lock()
	defer remote.mutex.Unlock()
	if mq == nil {
		return
	}
	offset, exist := remote.OffsetTable[mq.Topic]
	if !exist {
		return
	}
	rlog.Infof("delete: %s", mq.String())
	delete(offset, mq.QueueId)
}

func (remote *remoteBrokerOffsetStore) read(mq *kernel.MessageQueue, t readType) int64 {
	remote.mutex.RLock()
	if t == _ReadFromMemory || t == _ReadMemoryThenStore {
		off := readFromMemory(remote.OffsetTable, mq)
		if off >= 0 || (off == -1 && t == _ReadFromMemory) {
			remote.mutex.RUnlock()
			return off
		}
	}
	off, err := fetchConsumeOffsetFromBroker(remote.group, mq)
	if err != nil {
		rlog.Errorf("fetch offset of %s error: %s", mq.String(), err.Error())
		remote.mutex.RUnlock()
		return -1
	}
	remote.mutex.RUnlock()
	remote.update(mq, off, true)
	return off
}

func (remote *remoteBrokerOffsetStore) update(mq *kernel.MessageQueue, offset int64, increaseOnly bool) {
	rlog.Infof("update offset: %s to %d", mq, offset)
	remote.mutex.Lock()
	defer remote.mutex.Unlock()
	localOffset, exist := remote.OffsetTable[mq.Topic]
	if !exist {
		localOffset = make(map[int]*queueOffset)
		remote.OffsetTable[mq.Topic] = localOffset
	}
	q, exist := localOffset[mq.QueueId]
	if !exist {
		rlog.Infof("add a new queue: %s, off: %d", mq.String(), offset)
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

func readFromMemory(table map[string]map[int]*queueOffset, mq *kernel.MessageQueue) int64 {
	localOffset, exist := table[mq.Topic]
	if !exist {
		return -1
	}
	off, exist := localOffset[mq.QueueId]
	if !exist {
		return -1
	}

	return off.Offset
}

func fetchConsumeOffsetFromBroker(group string, mq *kernel.MessageQueue) (int64, error) {
	broker := kernel.FindBrokerAddrByName(mq.BrokerName)
	if broker == "" {
		kernel.UpdateTopicRouteInfo(mq.Topic)
		broker = kernel.FindBrokerAddrByName(mq.BrokerName)
	}
	if broker == "" {
		return int64(-1), fmt.Errorf("broker: %s address not found", mq.BrokerName)
	}
	queryOffsetRequest := &kernel.QueryConsumerOffsetRequest{
		ConsumerGroup: group,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
	}
	cmd := remote.NewRemotingCommand(kernel.ReqQueryConsumerOffset, queryOffsetRequest, nil)
	res, err := remote.InvokeSync(broker, cmd, 3*time.Second)
	if err != nil {
		return -1, err
	}
	if res.Code != kernel.ResSuccess {
		return -2, fmt.Errorf("broker response code: %d, remarks: %s", res.Code, res.Remark)
	}

	off, err := strconv.ParseInt(res.ExtFields["offset"], 10, 64)

	if err != nil {
		return -1, err
	}

	return off, nil
}

func updateConsumeOffsetToBroker(group, topic string, queue *queueOffset) error {
	broker := kernel.FindBrokerAddrByName(queue.Broker)
	if broker == "" {
		kernel.UpdateTopicRouteInfo(topic)
		broker = kernel.FindBrokerAddrByName(queue.Broker)
	}
	if broker == "" {
		return fmt.Errorf("broker: %s address not found", queue.Broker)
	}

	updateOffsetRequest := &kernel.UpdateConsumerOffsetRequest{
		ConsumerGroup: group,
		Topic:         topic,
		QueueId:       queue.QueueID,
		CommitOffset:  queue.Offset,
	}
	cmd := remote.NewRemotingCommand(kernel.ReqUpdateConsumerOffset, updateOffsetRequest, nil)
	return remote.InvokeOneWay(broker, cmd, 5*time.Second)
}
