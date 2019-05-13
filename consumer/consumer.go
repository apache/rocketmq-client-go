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
	"github.com/tidwall/gjson"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

// Message model defines the way how messages are delivered to each consumer clients.
// </p>
//
// RocketMQ supports two message models: clustering and broadcasting. If clustering is set, consumer clients with
// the same {@link #consumerGroup} would only consume shards of the messages subscribed, which achieves load
// balances; Conversely, if the broadcasting is set, each consumer client will consume all subscribed messages
// separately.
// </p>
//
// This field defaults to clustering.
type MessageModel int

const (
	BroadCasting MessageModel = iota
	Clustering
)

func (mode MessageModel) String() string {
	switch mode {
	case BroadCasting:
		return "BroadCasting"
	case Clustering:
		return "Clustering"
	default:
		return "Unknown"
	}
}

// Consuming point on consumer booting.
// </p>
//
// There are three consuming points:
// <ul>
// <li>
// <code>CONSUME_FROM_LAST_OFFSET</code>: consumer clients pick up where it stopped previously.
// If it were a newly booting up consumer client, according aging of the consumer group, there are two
// cases:
// <ol>
// <li>
// if the consumer group is created so recently that the earliest message being subscribed has yet
// expired, which means the consumer group represents a lately launched business, consuming will
// start from the very beginning;
// </li>
// <li>
// if the earliest message being subscribed has expired, consuming will start from the latest
// messages, meaning messages born prior to the booting timestamp would be ignored.
// </li>
// </ol>
// </li>
// <li>
// <code>CONSUME_FROM_FIRST_OFFSET</code>: Consumer client will start from earliest messages available.
// </li>
// <li>
// <code>CONSUME_FROM_TIMESTAMP</code>: Consumer client will start from specified timestamp, which means
// messages born prior to {@link #consumeTimestamp} will be ignored
// </li>
// </ul>
type ConsumeFromWhere int

const (
	ConsumeFromLastOffset ConsumeFromWhere = iota
	ConsumeFromFirstOffset
	ConsumeFromTimestamp
)

type ConsumeType string

const (
	_PullConsume = ConsumeType("pull")
	_PushConsume = ConsumeType("push")
)

type ExpressionType string

const (
	/**
	 * <ul>
	 * Keywords:
	 * <li>{@code AND, OR, NOT, BETWEEN, IN, TRUE, FALSE, IS, NULL}</li>
	 * </ul>
	 * <p/>
	 * <ul>
	 * Data type:
	 * <li>Boolean, like: TRUE, FALSE</li>
	 * <li>String, like: 'abc'</li>
	 * <li>Decimal, like: 123</li>
	 * <li>Float number, like: 3.1415</li>
	 * </ul>
	 * <p/>
	 * <ul>
	 * Grammar:
	 * <li>{@code AND, OR}</li>
	 * <li>{@code >, >=, <, <=, =}</li>
	 * <li>{@code BETWEEN A AND B}, equals to {@code >=A AND <=B}</li>
	 * <li>{@code NOT BETWEEN A AND B}, equals to {@code >B OR <A}</li>
	 * <li>{@code IN ('a', 'b')}, equals to {@code ='a' OR ='b'}, this operation only support String type.</li>
	 * <li>{@code IS NULL}, {@code IS NOT NULL}, check parameter whether is null, or not.</li>
	 * <li>{@code =TRUE}, {@code =FALSE}, check parameter whether is true, or false.</li>
	 * </ul>
	 * <p/>
	 * <p>
	 * Example:
	 * (a > 10 AND a < 100) OR (b IS NOT NULL AND b=TRUE)
	 * </p>
	 */
	SQL92 = ExpressionType("SQL92")

	/**
	 * Only support or operation such as
	 * "tag1 || tag2 || tag3", <br>
	 * If null or * expression, meaning subscribe all.
	 */
	TAG = ExpressionType("TAG")
)

func IsTagType(exp string) bool {
	if exp == "" || exp == "TAG" {
		return true
	}
	return false
}

const (
	_SubAll = "*"
)

type PullRequest struct {
	consumerGroup string
	mq            *kernel.MessageQueue
	pq            *processQueue
	nextOffset    int64
	lockedFirst   bool
}

func (pr *PullRequest) String() string {
	return fmt.Sprintf("[ConsumerGroup: %s, Topic: %s, MessageQueue: %d]",
		pr.consumerGroup, pr.mq.Topic, pr.mq.QueueId)
}

type ConsumerOption struct {
	kernel.ClientOption
	NameServerAddr string

	/**
	 * Backtracking consumption time with second precision. Time format is
	 * 20131223171201<br>
	 * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
	 * Default backtracking consumption time Half an hour ago.
	 */
	ConsumeTimestamp string

	// The socket timeout in milliseconds
	ConsumerPullTimeout time.Duration

	// Concurrently max span offset.it has no effect on sequential consumption
	ConsumeConcurrentlyMaxSpan int

	// Flow control threshold on queue level, each message queue will cache at most 1000 messages by default,
	// Consider the {PullBatchSize}, the instantaneous value may exceed the limit
	PullThresholdForQueue int64

	// Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
	// Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
	//
	// The size of a message only measured by message body, so it's not accurate
	PullThresholdSizeForQueue int

	// Flow control threshold on topic level, default value is -1(Unlimited)
	//
	// The value of {@code pullThresholdForQueue} will be overwrote and calculated based on
	// {@code pullThresholdForTopic} if it is't unlimited
	//
	// For example, if the value of pullThresholdForTopic is 1000 and 10 message queues are assigned to this consumer,
	// then pullThresholdForQueue will be set to 100
	PullThresholdForTopic int

	// Limit the cached message size on topic level, default value is -1 MiB(Unlimited)
	//
	// The value of {@code pullThresholdSizeForQueue} will be overwrote and calculated based on
	// {@code pullThresholdSizeForTopic} if it is't unlimited
	//
	// For example, if the value of pullThresholdSizeForTopic is 1000 MiB and 10 message queues are
	// assigned to this consumer, then pullThresholdSizeForQueue will be set to 100 MiB
	PullThresholdSizeForTopic int

	// Message pull Interval
	PullInterval time.Duration

	// Batch consumption size
	ConsumeMessageBatchMaxSize int

	// Batch pull size
	PullBatchSize int32

	// Whether update subscription relationship when every pull
	PostSubscriptionWhenPull bool

	// Max re-consume times. -1 means 16 times.
	//
	// If messages are re-consumed more than {@link #maxReconsumeTimes} before success, it's be directed to a deletion
	// queue waiting.
	MaxReconsumeTimes int

	// Suspending pulling time for cases requiring slow pulling like flow-control scenario.
	SuspendCurrentQueueTimeMillis time.Duration

	// Maximum amount of time a message may block the consuming thread.
	ConsumeTimeout time.Duration

	ConsumerModel  MessageModel
	Strategy       AllocateStrategy
	ConsumeOrderly bool
	FromWhere      ConsumeFromWhere
	// TODO traceDispatcher
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
	model          MessageModel
	allocate       func(string, string, []*kernel.MessageQueue, []string) []*kernel.MessageQueue
	unitMode       bool
	consumeOrderly bool
	fromWhere      ConsumeFromWhere

	cType     ConsumeType
	client    *kernel.RMQClient
	mqChanged func(topic string, mqAll, mqDivided []*kernel.MessageQueue)
	state     kernel.ServiceState
	pause     bool
	once      sync.Once
	option    ConsumerOption
	// key: int, hash(*kernel.MessageQueue)
	// value: *processQueue
	processQueueTable sync.Map

	// key: topic(string)
	// value: map[int]*kernel.MessageQueue
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
	mqs := make([]*kernel.MessageQueue, 0)
	dc.processQueueTable.Range(func(key, value interface{}) bool {
		mqs = append(mqs, key.(*kernel.MessageQueue))
		return true
	})
	dc.storage.persist(mqs)
}

func (dc *defaultConsumer) updateTopicSubscribeInfo(topic string, mqs []*kernel.MessageQueue) {
	_, exist := dc.subscriptionDataTable.Load(topic)
	// does subscribe, if true, replace it
	if exist {
		mqSet := make(map[int]*kernel.MessageQueue, 0)
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
		mqs := v.([]*kernel.MessageQueue)
		switch dc.model {
		case BroadCasting:
			changed := dc.updateProcessQueueTable(topic, mqs)
			if changed {
				dc.mqChanged(topic, mqs, mqs)
				rlog.Infof("messageQueueChanged, Group: %s, Topic: %s, MessageQueues: %v",
					dc.consumerGroup, topic, mqs)
			}
		case Clustering:
			cidAll := dc.findConsumerList(topic)
			if cidAll == nil {
				rlog.Warnf("do balance for Group: %s, Topic: %s get consumer id list failed",
					dc.consumerGroup, topic)
				return true
			}
			mqAll := make([]*kernel.MessageQueue, len(mqs))
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
				rlog.Infof("do balance result changed, allocateMessageQueueStrategyName=%s, group=%s, "+
					"topic=%s, clientId=%s, mqAllSize=%d, cidAllSize=%d, rebalanceResultSize=%d, "+
					"rebalanceResultSet=%v", string(dc.option.Strategy), dc.consumerGroup, topic, dc.client.ClientID(), len(mqAll),
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
	ConsumerGroup string                 `json:"consumerGroup"`
	ClientId      string                 `json:"clientId"`
	MQs           []*kernel.MessageQueue `json:"mqSet"`
}

func (dc *defaultConsumer) lock(mq *kernel.MessageQueue) bool {
	brokerResult := kernel.FindBrokerAddressInSubscribe(mq.BrokerName, kernel.MasterId, true)

	if brokerResult == nil {
		return false
	}

	body := &lockBatchRequestBody{
		ConsumerGroup: dc.consumerGroup,
		ClientId:      dc.client.ClientID(),
		MQs:           []*kernel.MessageQueue{mq},
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

func (dc *defaultConsumer) unlock(mq *kernel.MessageQueue, oneway bool) {
	brokerResult := kernel.FindBrokerAddressInSubscribe(mq.BrokerName, kernel.MasterId, true)

	if brokerResult == nil {
		return
	}

	body := &lockBatchRequestBody{
		ConsumerGroup: dc.consumerGroup,
		ClientId:      dc.client.ClientID(),
		MQs:           []*kernel.MessageQueue{mq},
	}
	dc.doUnlock(brokerResult.BrokerAddr, body, oneway)
	rlog.Warnf("unlock messageQueue. group:%s, clientId:%s, mq:%s",
		dc.consumerGroup, dc.client.ClientID(), mq.String())
}

func (dc *defaultConsumer) lockAll(mq kernel.MessageQueue) {
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

func (dc *defaultConsumer) doLock(addr string, body *lockBatchRequestBody) []kernel.MessageQueue {
	data, _ := json.Marshal(body)
	request := remote.NewRemotingCommand(kernel.ReqLockBatchMQ, nil, data)
	response, err := dc.client.InvokeSync(addr, request, 1*time.Second)
	if err != nil {
		rlog.Errorf("lock mq to broker: %s error %s", addr, err.Error())
		return nil
	}
	lockOKMQSet := struct {
		MQs []kernel.MessageQueue `json:"lockOKMQSet"`
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

func (dc *defaultConsumer) buildProcessQueueTableByBrokerName() map[string][]*kernel.MessageQueue {
	result := make(map[string][]*kernel.MessageQueue, 0)

	dc.processQueueTable.Range(func(key, value interface{}) bool {
		mq := key.(*kernel.MessageQueue)
		mqs, exist := result[mq.BrokerName]
		if !exist {
			mqs = make([]*kernel.MessageQueue, 0)
		}
		mqs = append(mqs, mq)
		result[mq.BrokerName] = mqs
		return true
	})

	return result
}

// TODO 问题不少 需要再好好对一下
func (dc *defaultConsumer) updateProcessQueueTable(topic string, mqs []*kernel.MessageQueue) bool {
	var changed bool
	mqSet := make(map[*kernel.MessageQueue]bool)
	for idx := range mqs {
		mqSet[mqs[idx]] = true
	}
	// TODO
	dc.processQueueTable.Range(func(key, value interface{}) bool {
		mq := key.(*kernel.MessageQueue)
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

func (dc *defaultConsumer) removeUnnecessaryMessageQueue(mq *kernel.MessageQueue, pq *processQueue) bool {
	dc.storage.persist([]*kernel.MessageQueue{mq})
	dc.storage.remove(mq)
	return true
}

func (dc *defaultConsumer) computePullFromWhere(mq *kernel.MessageQueue) int64 {
	if dc.cType == _PullConsume {
		return 0
	}
	var result = int64(-1)
	lastOffset := dc.storage.read(mq, _ReadFromStore)
	if lastOffset >= 0 {
		result = lastOffset
	} else {
		switch dc.fromWhere {
		case ConsumeFromLastOffset:
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
		case ConsumeFromFirstOffset:
			if lastOffset == -1 {
				result = 0
			}
		case ConsumeFromTimestamp:
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

func (dc *defaultConsumer) sendBack(msg *kernel.MessageExt, level int) error {
	return nil
}

// QueryMaxOffset with specific queueId and topic
func (dc *defaultConsumer) queryMaxOffset(mq *kernel.MessageQueue) (int64, error) {
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
func (dc *defaultConsumer) searchOffsetByTimestamp(mq *kernel.MessageQueue, timestamp int64) (int64, error) {
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

func buildSubscriptionData(topic string, selector MessageSelector) *kernel.SubscriptionData {
	subData := &kernel.SubscriptionData{
		Topic:     topic,
		SubString: selector.Expression,
		ExpType:   string(selector.Type),
	}

	if selector.Type != "" && selector.Type != TAG {
		return subData
	}

	if selector.Expression == "" || selector.Expression == _SubAll {
		subData.ExpType = string(TAG)
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

func getNextQueueOf(topic string) *kernel.MessageQueue {
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

func tryFindBroker(mq *kernel.MessageQueue) *kernel.FindBrokerResult {
	result := kernel.FindBrokerAddressInSubscribe(mq.BrokerName, recalculatePullFromWhichNode(mq), false)

	if result == nil {
		kernel.UpdateTopicRouteInfo(mq.Topic)
	}
	return kernel.FindBrokerAddressInSubscribe(mq.BrokerName, recalculatePullFromWhichNode(mq), false)
}

var (
	pullFromWhichNodeTable sync.Map
)

func updatePullFromWhichNode(mq *kernel.MessageQueue, brokerId int64) {
	pullFromWhichNodeTable.Store(mq.HashCode(), brokerId)
}

func recalculatePullFromWhichNode(mq *kernel.MessageQueue) int64 {
	v, exist := pullFromWhichNodeTable.Load(mq.HashCode())
	if exist {
		return v.(int64)
	}
	return kernel.MasterId
}
