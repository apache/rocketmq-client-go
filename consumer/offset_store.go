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
	"context"
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

//go:generate mockgen -source offset_store.go -destination mock_offset_store.go -self_package github.com/apache/rocketmq-client-go/consumer  --package consumer OffsetStore
type OffsetStore interface {
	persist(mqs []*primitive.MessageQueue)
	remove(mq *primitive.MessageQueue)
	read(mq *primitive.MessageQueue, t readType) int64
	update(mq *primitive.MessageQueue, offset int64, increaseOnly bool)
}

type OffsetSerializeWrapper struct {
	OffsetTable map[MessageQueueKey]int64 `json:"offsetTable"`
}

type MessageQueueKey primitive.MessageQueue

func (mq MessageQueueKey) MarshalText() (text []byte, err error) {
	repr := struct {
		Topic      string `json:"topic"`
		BrokerName string `json:"brokerName"`
		QueueId    int    `json:"queueId"`
	}{
		Topic:      mq.Topic,
		BrokerName: mq.BrokerName,
		QueueId:    mq.QueueId,
	}
	text, err = json.Marshal(repr)
	return
}

func (mq *MessageQueueKey) UnmarshalText(text []byte) error {
	repr := struct {
		Topic      string `json:"topic"`
		BrokerName string `json:"brokerName"`
		QueueId    int    `json:"queueId"`
	}{}
	err := json.Unmarshal(text, &repr)
	if err != nil {
		return err
	}
	mq.Topic = repr.Topic
	mq.QueueId = repr.QueueId
	mq.BrokerName = repr.BrokerName

	return nil
}

type localFileOffsetStore struct {
	group       string
	path        string
	OffsetTable map[MessageQueueKey]int64
	// mutex for offset file
	mutex sync.Mutex
}

func NewLocalFileOffsetStore(clientID, group string) OffsetStore {
	store := &localFileOffsetStore{
		group:       group,
		path:        filepath.Join(_LocalOffsetStorePath, clientID, group, "offset.json"),
		OffsetTable: make(map[MessageQueueKey]int64),
	}
	store.load()
	return store
}

func (local *localFileOffsetStore) load() {
	local.mutex.Lock()
	defer local.mutex.Unlock()
	data, err := utils.FileReadAll(local.path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		rlog.Errorf("read from store failed. err: %v \n", err)
		data, err = utils.FileReadAll(filepath.Join(local.path, ".bak"))
	}
	if err != nil {
		rlog.Debugf("load local offset: %s error: %s", local.path, err.Error())
		return
	}
	datas := make(map[MessageQueueKey]int64)

	wrapper := OffsetSerializeWrapper{
		OffsetTable: datas,
	}

	err = json.Unmarshal(data, &wrapper)
	if err != nil {
		rlog.Debugf("unmarshal local offset: %s error: %s", local.path, err.Error())
		return
	}

	if datas != nil {
		local.OffsetTable = datas
	}
}

func (local *localFileOffsetStore) read(mq *primitive.MessageQueue, t readType) int64 {
	switch t {
	case _ReadFromMemory, _ReadMemoryThenStore:
		off := readFromMemory(local.OffsetTable, mq)
		if off >= 0 || (off == -1 && t == _ReadFromMemory) {
			return off
		}
	case _ReadFromStore:
		local.load()
		return readFromMemory(local.OffsetTable, mq)
	default:

	}
	return -1
}

func (local *localFileOffsetStore) update(mq *primitive.MessageQueue, offset int64, increaseOnly bool) {
	local.mutex.Lock()
	defer local.mutex.Unlock()
	rlog.Debugf("update offset: %s to %d", mq, offset)
	key := MessageQueueKey(*mq)
	localOffset, exist := local.OffsetTable[key]
	if !exist {
		local.OffsetTable[key] = offset
		return
	}
	if increaseOnly {
		if localOffset < offset {
			local.OffsetTable[key] = offset
		}
	} else {
		local.OffsetTable[key] = offset
	}
}

func (local *localFileOffsetStore) persist(mqs []*primitive.MessageQueue) {
	if len(mqs) == 0 {
		return
	}
	local.mutex.Lock()
	defer local.mutex.Unlock()

	wrapper := OffsetSerializeWrapper{
		OffsetTable: local.OffsetTable,
	}

	data, _ := json.Marshal(wrapper)
	utils.CheckError(fmt.Sprintf("persist offset to %s", local.path), utils.WriteToFile(local.path, data))
}

func (local *localFileOffsetStore) remove(mq *primitive.MessageQueue) {
	// nothing to do
}

type remoteBrokerOffsetStore struct {
	group       string
	OffsetTable map[primitive.MessageQueue]int64 `json:"OffsetTable"`
	client      internal.RMQClient
	namesrv     internal.Namesrvs
	mutex       sync.RWMutex
}

func NewRemoteOffsetStore(group string, client internal.RMQClient, namesrv internal.Namesrvs) OffsetStore {
	return &remoteBrokerOffsetStore{
		group:       group,
		client:      client,
		namesrv:     namesrv,
		OffsetTable: make(map[primitive.MessageQueue]int64),
	}
}

func (r *remoteBrokerOffsetStore) persist(mqs []*primitive.MessageQueue) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if len(mqs) == 0 {
		return
	}

	used := make(map[primitive.MessageQueue]struct{}, 0)
	for _, mq := range mqs {
		used[*mq] = struct{}{}
	}

	for mq, off := range r.OffsetTable {
		if _, ok := used[mq]; !ok {
			delete(r.OffsetTable, mq)
			continue
		}
		err := r.updateConsumeOffsetToBroker(r.group, mq, off)
		if err != nil {
			rlog.Warnf("update offset to broker error: %s, group: %s, queue: %s, offset: %d",
				err.Error(), r.group, mq.String(), off)
		} else {
			rlog.Debugf("update offset to broker success, group: %s, topic: %s, queue: %v offset: %v", r.group, mq.Topic, mq, off)
		}
	}
}

func (r *remoteBrokerOffsetStore) remove(mq *primitive.MessageQueue) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.OffsetTable, *mq)
	rlog.Infof("delete queueID %v of brokerName: %v \n", mq.QueueId, mq.BrokerName)
}

func (r *remoteBrokerOffsetStore) read(mq *primitive.MessageQueue, t readType) int64 {
	r.mutex.RLock()
	switch t {
	case _ReadFromMemory, _ReadMemoryThenStore:
		defer r.mutex.RUnlock()
		off, exist := r.OffsetTable[*mq]
		if exist {
			return off
		}
		if t == _ReadFromMemory {
			return -1
		}
	case _ReadFromStore:
		off, err := r.fetchConsumeOffsetFromBroker(r.group, mq)
		if err != nil {
			rlog.Errorf("fetch offset of %s error: %s", mq.String(), err.Error())
			r.mutex.RUnlock()
			return -1
		}
		r.mutex.RUnlock()
		r.update(mq, off, true)
		return off
	default:
	}

	return -1
}

func (r *remoteBrokerOffsetStore) update(mq *primitive.MessageQueue, offset int64, increaseOnly bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	localOffset, exist := r.OffsetTable[*mq]
	if !exist {
		r.OffsetTable[*mq] = offset
		return
	}
	if increaseOnly {
		if localOffset < offset {
			r.OffsetTable[*mq] = offset
		}
	} else {
		r.OffsetTable[*mq] = offset
	}
}

func (r *remoteBrokerOffsetStore) fetchConsumeOffsetFromBroker(group string, mq *primitive.MessageQueue) (int64, error) {
	broker := r.namesrv.FindBrokerAddrByName(mq.BrokerName)
	if broker == "" {
		r.namesrv.UpdateTopicRouteInfo(mq.Topic)
		broker = r.namesrv.FindBrokerAddrByName(mq.BrokerName)
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
	res, err := r.client.InvokeSync(context.Background(), broker, cmd, 3*time.Second)
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

func (r *remoteBrokerOffsetStore) updateConsumeOffsetToBroker(group string, mq primitive.MessageQueue, off int64) error {
	broker := r.namesrv.FindBrokerAddrByName(mq.BrokerName)
	if broker == "" {
		r.namesrv.UpdateTopicRouteInfo(mq.Topic)
		broker = r.namesrv.FindBrokerAddrByName(mq.BrokerName)
	}
	if broker == "" {
		return fmt.Errorf("broker: %s address not found", mq.BrokerName)
	}

	updateOffsetRequest := &internal.UpdateConsumerOffsetRequest{
		ConsumerGroup: group,
		Topic:         mq.Topic,
		QueueId:       mq.QueueId,
		CommitOffset:  off,
	}
	cmd := remote.NewRemotingCommand(internal.ReqUpdateConsumerOffset, updateOffsetRequest, nil)
	return r.client.InvokeOneWay(context.Background(), broker, cmd, 5*time.Second)
}

func readFromMemory(table map[MessageQueueKey]int64, mq *primitive.MessageQueue) int64 {
	localOffset, exist := table[MessageQueueKey(*mq)]
	if !exist {
		return -1
	}

	return localOffset
}
