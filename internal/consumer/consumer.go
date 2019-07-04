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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/rocketmq-client-go/internal/kernel"
	"github.com/apache/rocketmq-client-go/internal/remote"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/apache/rocketmq-client-go/utils"
	"github.com/tidwall/gjson"
)

const (
	// Delay some time when exception error
	_PullDelayTimeWhenError = 3 * time.Second

	// Flow control interval
	_PullDelayTimeWhenFlowControl = 50 * time.Millisecond

	// Delay some time when suspend pull service
	_PullDelayTimeWhenSuspend = 30 * time.Second

	// Long polling mode, the Consumer connection max suspend time
	_BrokerSuspendMaxTime = 20 * time.Second

	// Long polling mode, the Consumer connection timeout (must greater than _BrokerSuspendMaxTime)
	_ConsumerTimeoutWhenSuspend = 30 * time.Second

	// Offset persistent interval for consumer
	_PersistConsumerOffsetInterval = 5 * time.Second
)

type ConsumeType string

const (
	_PullConsume = ConsumeType("pull")
	_PushConsume = ConsumeType("push")

	_SubAll = "*"
)

type PullRequest struct {
	consumerGroup string
	mq            *primitive.MessageQueue
	pq            *processQueue
	nextOffset    int64
	lockedFirst   bool
}

func (pr *PullRequest) String() string {
	return fmt.Sprintf("[ConsumerGroup: %s, Topic: %s, MessageQueue: %d]",
		pr.consumerGroup, pr.mq.Topic, pr.mq.QueueId)
}

// TODO hook
type defaultConsumer struct {
	/**
	 * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
	 * load balance. It's required and needs to be globally unique.
	 * </p>
	 *
	 * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
	 */
	consumerGroup  string
	model          primitive.MessageModel
	allocate       func(string, string, []*primitive.MessageQueue, []string) []*primitive.MessageQueue
	unitMode       bool
	consumeOrderly bool
	fromWhere      primitive.ConsumeFromWhere

	cType     ConsumeType
	client    *kernel.RMQClient
	mqChanged func(topic string, mqAll, mqDivided []*primitive.MessageQueue)
	state     kernel.ServiceState
	pause     bool
	once      sync.Once
	option    primitive.ConsumerOptions
	// key: int, hash(*primitive.MessageQueue)
	// value: *processQueue
	processQueueTable sync.Map

	// key: topic(string)
	// value: map[int]*primitive.MessageQueue
	topicSubscribeInfoTable sync.Map

	// key: topic
	// value: *SubscriptionData
	subscriptionDataTable sync.Map
	storage               OffsetStore
	// chan for push consumer
	prCh chan PullRequest
}

func (dc *defaultConsumer) persistConsumerOffset() {
	err := dc.makeSureStateOK()
	if err != nil {
		rlog.Errorf("consumer state error: %s", err.Error())
		return
	}
	mqs := make([]*primitive.MessageQueue, 0)
	dc.processQueueTable.Range(func(key, value interface{}) bool {
		mqs = append(mqs, key.(*primitive.MessageQueue))
		return true
	})
	dc.storage.persist(mqs)
}

func (dc *defaultConsumer) updateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue) {
	_, exist := dc.subscriptionDataTable.Load(topic)
	// does subscribe, if true, replace it
	if exist {
		mqSet := make(map[int]*primitive.MessageQueue, 0)
		for idx := range mqs {
			mq := mqs[idx]
			mqSet[mq.HashCode()] = mq
		}
		dc.topicSubscribeInfoTable.Store(topic, mqs)
	}
}

func (dc *defaultConsumer) isSubscribeTopicNeedUpdate(topic string) bool {
	_, exist := dc.subscriptionDataTable.Load(topic)
	if !exist {
		return false
	}
	_, exist = dc.topicSubscribeInfoTable.Load(topic)
	return !exist
}

func (dc *defaultConsumer) doBalance() {
	dc.subscriptionDataTable.Range(func(key, value interface{}) bool {
		topic := key.(string)
		if strings.HasPrefix(topic, kernel.RetryGroupTopicPrefix) {
			return true
		}
		v, exist := dc.topicSubscribeInfoTable.Load(topic)
		if !exist {
			rlog.Warnf("do balance of group: %s, but topic: %s does not exist.", dc.consumerGroup, topic)
			return true
		}
		mqs := v.([]*primitive.MessageQueue)
		switch dc.model {
		case primitive.BroadCasting:
			changed := dc.updateProcessQueueTable(topic, mqs)
			if changed {
				dc.mqChanged(topic, mqs, mqs)
				rlog.Infof("messageQueueChanged, Group: %s, Topic: %s, MessageQueues: %v",
					dc.consumerGroup, topic, mqs)
			}
		case primitive.Clustering:
			cidAll := dc.findConsumerList(topic)
			if cidAll == nil {
				rlog.Warnf("do balance for Group: %s, Topic: %s get consumer id list failed",
					dc.consumerGroup, topic)
				return true
			}
			mqAll := make([]*primitive.MessageQueue, len(mqs))
			copy(mqAll, mqs)
			sort.Strings(cidAll)
			sort.SliceStable(mqAll, func(i, j int) bool {
				v := strings.Compare(mqAll[i].Topic, mqAll[j].Topic)
				if v != 0 {
					return v > 0
				}

				v = strings.Compare(mqAll[i].BrokerName, mqAll[j].BrokerName)
				if v != 0 {
					return v > 0
				}
				return (mqAll[i].QueueId - mqAll[j].QueueId) < 0
			})
			allocateResult := dc.allocate(dc.consumerGroup, dc.client.ClientID(), mqAll, cidAll)
			changed := dc.updateProcessQueueTable(topic, allocateResult)
			if changed {
				dc.mqChanged(topic, mqAll, allocateResult)
				rlog.Infof("do balance result changed, group=%s, "+
					"topic=%s, clientId=%s, mqAllSize=%d, cidAllSize=%d, rebalanceResultSize=%d, "+
					"rebalanceResultSet=%v", dc.consumerGroup, topic, dc.client.ClientID(), len(mqAll),
					len(cidAll), len(allocateResult), allocateResult)

			}
		}
		return true
	})
}

func (dc *defaultConsumer) SubscriptionDataList() []*kernel.SubscriptionData {
	result := make([]*kernel.SubscriptionData, 0)
	dc.subscriptionDataTable.Range(func(key, value interface{}) bool {
		result = append(result, value.(*kernel.SubscriptionData))
		return true
	})
	return result
}

func (dc *defaultConsumer) makeSureStateOK() error {
	if dc.state != kernel.StateRunning {
		return fmt.Errorf("state not running, actually: %v", dc.state)
	}
	return nil
}

type lockBatchRequestBody struct {
	ConsumerGroup string                    `json:"consumerGroup"`
	ClientId      string                    `json:"clientId"`
	MQs           []*primitive.MessageQueue `json:"mqSet"`
}

func (dc *defaultConsumer) lock(mq *primitive.MessageQueue) bool {
	brokerResult := kernel.FindBrokerAddressInSubscribe(mq.BrokerName, kernel.MasterId, true)

	if brokerResult == nil {
		return false
	}

	body := &lockBatchRequestBody{
		ConsumerGroup: dc.consumerGroup,
		ClientId:      dc.client.ClientID(),
		MQs:           []*primitive.MessageQueue{mq},
	}
	lockedMQ := dc.doLock(brokerResult.BrokerAddr, body)
	var lockOK bool
	for idx := range lockedMQ {
		_mq := lockedMQ[idx]
		v, exist := dc.processQueueTable.Load(_mq)
		if exist {
			pq := v.(*processQueue)
			pq.locked = true
			pq.lastConsumeTime = time.Now()
		}
		if _mq.Equals(mq) {
			lockOK = true
		}
	}
	rlog.Infof("the message queue lock %v, %s %s", lockOK, dc.consumerGroup, mq.String())
	return lockOK
}

func (dc *defaultConsumer) unlock(mq *primitive.MessageQueue, oneway bool) {
	brokerResult := kernel.FindBrokerAddressInSubscribe(mq.BrokerName, kernel.MasterId, true)

	if brokerResult == nil {
		return
	}

	body := &lockBatchRequestBody{
		ConsumerGroup: dc.consumerGroup,
		ClientId:      dc.client.ClientID(),
		MQs:           []*primitive.MessageQueue{mq},
	}
	dc.doUnlock(brokerResult.BrokerAddr, body, oneway)
	rlog.Warnf("unlock messageQueue. group:%s, clientId:%s, mq:%s",
		dc.consumerGroup, dc.client.ClientID(), mq.String())
}

func (dc *defaultConsumer) lockAll(mq primitive.MessageQueue) {
	mqMapSet := dc.buildProcessQueueTableByBrokerName()
	for broker, mqs := range mqMapSet {
		if len(mqs) == 0 {
			continue
		}
		brokerResult := kernel.FindBrokerAddressInSubscribe(broker, kernel.MasterId, true)
		if brokerResult == nil {
			continue
		}
		body := &lockBatchRequestBody{
			ConsumerGroup: dc.consumerGroup,
			ClientId:      dc.client.ClientID(),
			MQs:           mqs,
		}
		lockedMQ := dc.doLock(brokerResult.BrokerAddr, body)
		set := make(map[int]bool, 0)
		for idx := range lockedMQ {
			_mq := lockedMQ[idx]
			v, exist := dc.processQueueTable.Load(_mq)
			if exist {
				pq := v.(*processQueue)
				pq.locked = true
				pq.lastConsumeTime = time.Now()
			}
			set[_mq.HashCode()] = true
		}
		for idx := range mqs {
			_mq := mqs[idx]
			if !set[_mq.HashCode()] {
				v, exist := dc.processQueueTable.Load(_mq)
				if exist {
					pq := v.(*processQueue)
					pq.locked = true
					pq.lastLockTime = time.Now()
					rlog.Warnf("the message queue: %s locked Failed, Group: %s", mq.String(), dc.consumerGroup)
				}
			}
		}
	}
}

func (dc *defaultConsumer) unlockAll(oneway bool) {
	mqMapSet := dc.buildProcessQueueTableByBrokerName()
	for broker, mqs := range mqMapSet {
		if len(mqs) == 0 {
			continue
		}
		brokerResult := kernel.FindBrokerAddressInSubscribe(broker, kernel.MasterId, true)
		if brokerResult == nil {
			continue
		}
		body := &lockBatchRequestBody{
			ConsumerGroup: dc.consumerGroup,
			ClientId:      dc.client.ClientID(),
			MQs:           mqs,
		}
		dc.doUnlock(brokerResult.BrokerAddr, body, oneway)
		for idx := range mqs {
			_mq := mqs[idx]
			v, exist := dc.processQueueTable.Load(_mq)
			if exist {
				v.(*processQueue).locked = false
				rlog.Warnf("the message queue: %s locked Failed, Group: %s", _mq.String(), dc.consumerGroup)
			}
		}
	}
}

func (dc *defaultConsumer) doLock(addr string, body *lockBatchRequestBody) []primitive.MessageQueue {
	data, _ := json.Marshal(body)
	request := remote.NewRemotingCommand(kernel.ReqLockBatchMQ, nil, data)
	response, err := dc.client.InvokeSync(addr, request, 1*time.Second)
	if err != nil {
		rlog.Errorf("lock mq to broker: %s error %s", addr, err.Error())
		return nil
	}
	lockOKMQSet := struct {
		MQs []primitive.MessageQueue `json:"lockOKMQSet"`
	}{}
	err = json.Unmarshal(response.Body, &lockOKMQSet)
	if err != nil {
		rlog.Errorf("Unmarshal lock mq body error %s", err.Error())
		return nil
	}
	return lockOKMQSet.MQs
}

func (dc *defaultConsumer) doUnlock(addr string, body *lockBatchRequestBody, oneway bool) {
	data, _ := json.Marshal(body)
	request := remote.NewRemotingCommand(kernel.ReqUnlockBatchMQ, nil, data)
	if oneway {
		err := dc.client.InvokeOneWay(addr, request, 3*time.Second)
		if err != nil {
			rlog.Errorf("lock mq to broker with oneway: %s error %s", addr, err.Error())
		}
	} else {
		response, err := dc.client.InvokeSync(addr, request, 1*time.Second)
		if err != nil {
			rlog.Errorf("lock mq to broker: %s error %s", addr, err.Error())
		}
		if response.Code != kernel.ResSuccess {
			// TODO error
		}
	}
}

func (dc *defaultConsumer) buildProcessQueueTableByBrokerName() map[string][]*primitive.MessageQueue {
	result := make(map[string][]*primitive.MessageQueue, 0)

	dc.processQueueTable.Range(func(key, value interface{}) bool {
		mq := key.(*primitive.MessageQueue)
		mqs, exist := result[mq.BrokerName]
		if !exist {
			mqs = make([]*primitive.MessageQueue, 0)
		}
		mqs = append(mqs, mq)
		result[mq.BrokerName] = mqs
		return true
	})

	return result
}

// TODO 问题不少 需要再好好对一下
func (dc *defaultConsumer) updateProcessQueueTable(topic string, mqs []*primitive.MessageQueue) bool {
	var changed bool
	mqSet := make(map[*primitive.MessageQueue]bool)
	for idx := range mqs {
		mqSet[mqs[idx]] = true
	}
	// TODO
	dc.processQueueTable.Range(func(key, value interface{}) bool {
		mq := key.(*primitive.MessageQueue)
		pq := value.(*processQueue)
		if mq.Topic == topic {
			if !mqSet[mq] {
				pq.dropped = true
				if dc.removeUnnecessaryMessageQueue(mq, pq) {
					//delete(mqSet, mq)
					dc.processQueueTable.Delete(key)
					changed = true
					rlog.Infof("do defaultConsumer, Group:%s, remove unnecessary mq: %s", dc.consumerGroup, mq.String())
				}
			} else if pq.isPullExpired() && dc.cType == _PushConsume {
				pq.dropped = true
				if dc.removeUnnecessaryMessageQueue(mq, pq) {
					delete(mqSet, mq)
					changed = true
					rlog.Infof("do defaultConsumer, Group:%s, remove unnecessary mq: %s, "+
						"because pull was paused, so try to fixed it", dc.consumerGroup, mq)
				}
			}
		}
		return true
	})

	if dc.cType == _PushConsume {
		for mq := range mqSet {
			_, exist := dc.processQueueTable.Load(mq)
			if exist {
				continue
			}
			if dc.consumeOrderly && !dc.lock(mq) {
				rlog.Warnf("do defaultConsumer, Group:%s add a new mq failed, %s, because lock failed",
					dc.consumerGroup, mq.String())
				continue
			}
			dc.storage.remove(mq)
			nextOffset := dc.computePullFromWhere(mq)
			if nextOffset >= 0 {
				_, exist := dc.processQueueTable.Load(mq)
				if exist {
					rlog.Debugf("do defaultConsumer, Group: %s, mq already exist, %s", dc.consumerGroup, mq.String())
				} else {
					rlog.Infof("do defaultConsumer, Group: %s, add a new mq, %s", dc.consumerGroup, mq.String())
					pq := newProcessQueue()
					dc.processQueueTable.Store(mq, pq)
					pr := PullRequest{
						consumerGroup: dc.consumerGroup,
						mq:            mq,
						pq:            pq,
						nextOffset:    nextOffset,
					}
					dc.prCh <- pr
					changed = true
				}
			} else {
				rlog.Warnf("do defaultConsumer failed, Group:%s, add new mq failed, {}", dc.consumerGroup, mq)
			}
		}
	}

	return changed
}

func (dc *defaultConsumer) removeUnnecessaryMessageQueue(mq *primitive.MessageQueue, pq *processQueue) bool {
	dc.storage.persist([]*primitive.MessageQueue{mq})
	dc.storage.remove(mq)
	return true
}

func (dc *defaultConsumer) computePullFromWhere(mq *primitive.MessageQueue) int64 {
	if dc.cType == _PullConsume {
		return 0
	}
	var result = int64(-1)
	lastOffset := dc.storage.read(mq, _ReadFromStore)
	if lastOffset >= 0 {
		result = lastOffset
	} else {
		switch dc.fromWhere {
		case primitive.ConsumeFromLastOffset:
			if lastOffset == -1 {
				if strings.HasPrefix(mq.Topic, kernel.RetryGroupTopicPrefix) {
					lastOffset = 0
				} else {
					lastOffset, err := dc.queryMaxOffset(mq)
					if err == nil {
						result = lastOffset
					} else {
						rlog.Warnf("query max offset of: [%s:%d] error, %s", mq.Topic, mq.QueueId, err.Error())
					}
				}
			} else {
				result = -1
			}
		case primitive.ConsumeFromFirstOffset:
			if lastOffset == -1 {
				result = 0
			}
		case primitive.ConsumeFromTimestamp:
			if lastOffset == -1 {
				if strings.HasPrefix(mq.Topic, kernel.RetryGroupTopicPrefix) {
					lastOffset, err := dc.queryMaxOffset(mq)
					if err == nil {
						result = lastOffset
					} else {
						result = -1
						rlog.Warnf("query max offset of: [%s:%d] error, %s", mq.Topic, mq.QueueId, err.Error())
					}
				} else {
					t, err := time.Parse("20060102150405", dc.option.ConsumeTimestamp)
					if err != nil {
						result = -1
					} else {
						lastOffset, err := dc.searchOffsetByTimestamp(mq, t.Unix())
						if err != nil {
							result = -1
						} else {
							result = lastOffset
						}
					}
				}
			}
		default:
		}
	}

	return result
}

func (dc *defaultConsumer) findConsumerList(topic string) []string {
	brokerAddr := kernel.FindBrokerAddrByTopic(topic)
	if brokerAddr == "" {
		kernel.UpdateTopicRouteInfo(topic)
		brokerAddr = kernel.FindBrokerAddrByTopic(topic)
	}

	if brokerAddr != "" {
		req := &kernel.GetConsumerList{
			ConsumerGroup: dc.consumerGroup,
		}
		cmd := remote.NewRemotingCommand(kernel.ReqGetConsumerListByGroup, req, nil)
		res, err := dc.client.InvokeSync(brokerAddr, cmd, 3*time.Second) // TODO 超时机制有问题
		if err != nil {
			rlog.Errorf("get consumer list of [%s] from %s error: %s", dc.consumerGroup, brokerAddr, err.Error())
			return nil
		}
		result := gjson.ParseBytes(res.Body)
		list := make([]string, 0)
		arr := result.Get("consumerIdList").Array()
		for idx := range arr {
			list = append(list, arr[idx].String())
		}
		return list
	}
	return nil
}

func (dc *defaultConsumer) sendBack(msg *primitive.MessageExt, level int) error {
	return nil
}

// QueryMaxOffset with specific queueId and topic
func (dc *defaultConsumer) queryMaxOffset(mq *primitive.MessageQueue) (int64, error) {
	brokerAddr := kernel.FindBrokerAddrByName(mq.BrokerName)
	if brokerAddr == "" {
		kernel.UpdateTopicRouteInfo(mq.Topic)
		brokerAddr = kernel.FindBrokerAddrByName(mq.Topic)
	}
	if brokerAddr == "" {
		return -1, fmt.Errorf("the broker [%s] does not exist", mq.BrokerName)
	}

	request := &kernel.GetMaxOffsetRequest{
		Topic:   mq.Topic,
		QueueId: mq.QueueId,
	}

	cmd := remote.NewRemotingCommand(kernel.ReqGetMaxOffset, request, nil)
	response, err := dc.client.InvokeSync(brokerAddr, cmd, 3*time.Second)
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(response.ExtFields["offset"], 10, 64)
}

// SearchOffsetByTimestamp with specific queueId and topic
func (dc *defaultConsumer) searchOffsetByTimestamp(mq *primitive.MessageQueue, timestamp int64) (int64, error) {
	brokerAddr := kernel.FindBrokerAddrByName(mq.BrokerName)
	if brokerAddr == "" {
		kernel.UpdateTopicRouteInfo(mq.Topic)
		brokerAddr = kernel.FindBrokerAddrByName(mq.Topic)
	}
	if brokerAddr == "" {
		return -1, fmt.Errorf("the broker [%s] does not exist", mq.BrokerName)
	}

	request := &kernel.SearchOffsetRequest{
		Topic:     mq.Topic,
		QueueId:   mq.QueueId,
		Timestamp: timestamp,
	}

	cmd := remote.NewRemotingCommand(kernel.ReqSearchOffsetByTimestamp, request, nil)
	response, err := dc.client.InvokeSync(brokerAddr, cmd, 3*time.Second)
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(response.ExtFields["offset"], 10, 64)
}

func buildSubscriptionData(topic string, selector primitive.MessageSelector) *kernel.SubscriptionData {
	subData := &kernel.SubscriptionData{
		Topic:     topic,
		SubString: selector.Expression,
		ExpType:   string(selector.Type),
	}

	if selector.Type != "" && selector.Type != primitive.TAG {
		return subData
	}

	if selector.Expression == "" || selector.Expression == _SubAll {
		subData.ExpType = string(primitive.TAG)
		subData.SubString = _SubAll
	} else {
		tags := strings.Split(selector.Expression, "\\|\\|")
		for idx := range tags {
			trimString := strings.Trim(tags[idx], " ")
			if trimString != "" {
				if !subData.Tags[trimString] {
					subData.Tags[trimString] = true
				}
				hCode := utils.HashString(trimString)
				if !subData.Codes[int32(hCode)] {
					subData.Codes[int32(hCode)] = true
				}
			}
		}
	}
	return subData
}

func getNextQueueOf(topic string) *primitive.MessageQueue {
	queues, err := kernel.FetchSubscribeMessageQueues(topic)
	if err != nil && len(queues) > 0 {
		rlog.Error(err.Error())
		return nil
	}
	var index int64
	v, exist := queueCounterTable.Load(topic)
	if !exist {
		index = -1
		queueCounterTable.Store(topic, 0)
	} else {
		index = v.(int64)
	}

	return queues[int(atomic.AddInt64(&index, 1))%len(queues)]
}

func buildSysFlag(commitOffset, suspend, subscription, classFilter bool) int32 {
	var flag int32 = 0
	if commitOffset {
		flag |= 0x1 << 0
	}

	if suspend {
		flag |= 0x1 << 1
	}

	if subscription {
		flag |= 0x1 << 2
	}

	if classFilter {
		flag |= 0x1 << 3
	}

	return flag
}

func clearCommitOffsetFlag(sysFlag int32) int32 {
	return sysFlag & (^0x1 << 0)
}

func tryFindBroker(mq *primitive.MessageQueue) *kernel.FindBrokerResult {
	result := kernel.FindBrokerAddressInSubscribe(mq.BrokerName, recalculatePullFromWhichNode(mq), false)

	if result == nil {
		kernel.UpdateTopicRouteInfo(mq.Topic)
	}
	return kernel.FindBrokerAddressInSubscribe(mq.BrokerName, recalculatePullFromWhichNode(mq), false)
}

var (
	pullFromWhichNodeTable sync.Map
)

func updatePullFromWhichNode(mq *primitive.MessageQueue, brokerId int64) {
	pullFromWhichNodeTable.Store(mq.HashCode(), brokerId)
}

func recalculatePullFromWhichNode(mq *primitive.MessageQueue) int64 {
	v, exist := pullFromWhichNodeTable.Load(mq.HashCode())
	if exist {
		return v.(int64)
	}
	return kernel.MasterId
}
