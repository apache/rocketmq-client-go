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
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/internal"
	"github.com/apache/rocketmq-client-go/internal/remote"
	"github.com/apache/rocketmq-client-go/internal/utils"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
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
		_LocalOffsetStorePath = filepath.Join(os.Getenv("HOME"), ".rocketmq_client_go")
	}
}

type OffsetStore interface {
	persist(mqs []*primitive.MessageQueue)
	remove(mq *primitive.MessageQueue)
	read(mq *primitive.MessageQueue, t readType) int64
	update(mq *primitive.MessageQueue, offset int64, increaseOnly bool)
}

type localFileOffsetStore struct {
	group       string
	path        string
	OffsetTable map[string]map[int]*queueOffset
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
	if os.IsNotExist(err) {
		local.OffsetTable = make(map[string]map[int]*queueOffset)
		return
	}
	if err != nil {
		data, err = utils.FileReadAll(filepath.Join(local.path, ".bak"))
	}
	if err != nil {
		rlog.Debugf("load local offset: %s error: %s", local.path, err.Error())
		return
	}
	datas := make(map[string]map[int]*queueOffset)

	err = json.Unmarshal(data, &datas)
	if datas != nil {
		local.OffsetTable = datas
	} else {
		local.OffsetTable = make(map[string]map[int]*queueOffset)
	}
	if err != nil {
		rlog.Debugf("unmarshal local offset: %s error: %s", local.path, err.Error())
		return
	}
}

func (local *localFileOffsetStore) read(mq *primitive.MessageQueue, t readType) int64 {
	if t == _ReadFromMemory || t == _ReadMemoryThenStore {
		off := readFromMemory(local.OffsetTable, mq)
		if off >= 0 || (off == -1 && t == _ReadFromMemory) {
			return off
		}
	}
	local.load()
	return readFromMemory(local.OffsetTable, mq)
}

func (local *localFileOffsetStore) update(mq *primitive.MessageQueue, offset int64, increaseOnly bool) {
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

func (local *localFileOffsetStore) persist(mqs []*primitive.MessageQueue) {
	if len(mqs) == 0 {
		return
	}
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
			table[mq.Topic] = offsets
		}
		offsets[off.QueueID] = off
	}

	data, _ := json.Marshal(table)
	utils.CheckError(fmt.Sprintf("persist offset to %s", local.path), utils.WriteToFile(local.path, data))
}

func (local *localFileOffsetStore) remove(mq *primitive.MessageQueue) {
	// nothing to do
}

type remoteBrokerOffsetStore struct {
	group       string
	OffsetTable map[string]map[int]*queueOffset `json:"OffsetTable"`
	client      internal.RMQClient
	mutex       sync.RWMutex
}

func NewRemoteOffsetStore(group string, client internal.RMQClient) OffsetStore {
	return &remoteBrokerOffsetStore{
		group:       group,
		client:      client,
		OffsetTable: make(map[string]map[int]*queueOffset),
	}
}

func (r *remoteBrokerOffsetStore) persist(mqs []*primitive.MessageQueue) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if len(mqs) == 0 {
		return
	}
	for idx := range mqs {
		mq := mqs[idx]
		offsets, exist := r.OffsetTable[mq.Topic]
		if !exist {
			continue
		}
		off, exist := offsets[mq.QueueId]
		if !exist {
			continue
		}

		err := r.updateConsumeOffsetToBroker(r.group, mq.Topic, off)
		if err != nil {
			rlog.Warnf("update offset to broker error: %s, group: %s, queue: %s, offset: %d",
				err.Error(), r.group, mq.String(), off.Offset)
		} else {
			rlog.Debugf("update offset to broker success, group: %s, topic: %s, queue: %v", r.group, mq.Topic, off)
		}
	}
}

func (r *remoteBrokerOffsetStore) remove(mq *primitive.MessageQueue) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if mq == nil {
		return
	}
	offset, exist := r.OffsetTable[mq.Topic]
	if !exist {
		return
	}
	rlog.Infof("delete: %s", mq.String())
	delete(offset, mq.QueueId)
}

func (r *remoteBrokerOffsetStore) read(mq *primitive.MessageQueue, t readType) int64 {
	r.mutex.RLock()
	if t == _ReadFromMemory || t == _ReadMemoryThenStore {
		off := readFromMemory(r.OffsetTable, mq)
		if off >= 0 || (off == -1 && t == _ReadFromMemory) {
			r.mutex.RUnlock()
			return off
		}
	}
	off, err := r.fetchConsumeOffsetFromBroker(r.group, mq)
	if err != nil {
		rlog.Errorf("fetch offset of %s error: %s", mq.String(), err.Error())
		r.mutex.RUnlock()
		return -1
	}
	r.mutex.RUnlock()
	r.update(mq, off, true)
	return off
}

func (r *remoteBrokerOffsetStore) update(mq *primitive.MessageQueue, offset int64, increaseOnly bool) {
	rlog.Debugf("update offset: %s to %d", mq, offset)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	localOffset, exist := r.OffsetTable[mq.Topic]
	if !exist {
		localOffset = make(map[int]*queueOffset)
		r.OffsetTable[mq.Topic] = localOffset
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

func (r *remoteBrokerOffsetStore) fetchConsumeOffsetFromBroker(group string, mq *primitive.MessageQueue) (int64, error) {
	broker := internal.FindBrokerAddrByName(mq.BrokerName)
	if broker == "" {
		internal.UpdateTopicRouteInfo(mq.Topic)
		broker = internal.FindBrokerAddrByName(mq.BrokerName)
	}
	if broker == "" {
		return int64(-1), fmt.Errorf("broker: %s address not found", mq.BrokerName)
	}
	queryOffsetRequest := &internal.QueryConsumerOffsetRequest{
		ConsumerGroup: group,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
	}
	cmd := remote.NewRemotingCommand(internal.ReqQueryConsumerOffset, queryOffsetRequest, nil)
	res, err := r.client.InvokeSync(broker, cmd, 3*time.Second)
	if err != nil {
		return -1, err
	}
	if res.Code != internal.ResSuccess {
		return -2, fmt.Errorf("broker response code: %d, remarks: %s", res.Code, res.Remark)
	}

	off, err := strconv.ParseInt(res.ExtFields["offset"], 10, 64)

	if err != nil {
		return -1, err
	}

	return off, nil
}

func (r *remoteBrokerOffsetStore) updateConsumeOffsetToBroker(group, topic string, queue *queueOffset) error {
	broker := internal.FindBrokerAddrByName(queue.Broker)
	if broker == "" {
		internal.UpdateTopicRouteInfo(topic)
		broker = internal.FindBrokerAddrByName(queue.Broker)
	}
	if broker == "" {
		return fmt.Errorf("broker: %s address not found", queue.Broker)
	}

	updateOffsetRequest := &internal.UpdateConsumerOffsetRequest{
		ConsumerGroup: group,
		Topic:         topic,
		QueueId:       queue.QueueID,
		CommitOffset:  queue.Offset,
	}
	cmd := remote.NewRemotingCommand(internal.ReqUpdateConsumerOffset, updateOffsetRequest, nil)
	return r.client.InvokeOneWay(broker, cmd, 5*time.Second)
}

func readFromMemory(table map[string]map[int]*queueOffset, mq *primitive.MessageQueue) int64 {
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
