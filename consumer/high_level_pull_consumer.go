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
	"fmt"
	errors2 "github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type defaultHighLevelPullConsumer struct {
	*defaultManualPullConsumer
	consumerGroup           string
	model                   MessageModel
	namesrv                 internal.Namesrvs
	option                  consumerOptions
	client                  internal.RMQClient
	interceptor             primitive.Interceptor
	pullFromWhichNodeTable  sync.Map
	storage                 OffsetStore
	cType                   ConsumeType
	state                   int32
	subscriptionDataTable   sync.Map
	allocate                func(string, string, []*primitive.MessageQueue, []string) []*primitive.MessageQueue
	once                    sync.Once
	unitMode                bool
	topicSubscribeInfoTable sync.Map
	subscribedTopic         map[string]string
	closeOnce               sync.Once
	done                    chan struct{}
	consumerStartTimestamp  int64
	processQueueTable       sync.Map
	fromWhere               ConsumeFromWhere
	stat                    *StatsManager
	consumerMap             sync.Map
}

func NewHighLevelPullConsumer(options ...Option) (*defaultHighLevelPullConsumer, error) {
	defaultOpts := defaultHighLevelPullConsumerOptions()
	for _, apply := range options {
		apply(&defaultOpts)
	}

	srvs, err := internal.NewNamesrv(defaultOpts.Resolver)
	if err != nil {
		return nil, errors.Wrap(err, "new Namesrv failed.")
	}

	defaultOpts.Namesrv = srvs

	if defaultOpts.Namespace != "" {
		defaultOpts.GroupName = defaultOpts.Namespace + "%" + defaultOpts.GroupName
	}

	dc := &defaultManualPullConsumer{
		client:  internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil),
		option:  defaultOpts,
		namesrv: srvs,
	}

	hpc := &defaultHighLevelPullConsumer{
		client:                    internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil),
		consumerGroup:             defaultOpts.GroupName,
		defaultManualPullConsumer: dc,
		cType:                     _PullConsume,
		state:                     int32(internal.StateCreateJust),
		model:                     defaultOpts.ConsumerModel,
		allocate:                  defaultOpts.Strategy,
		namesrv:                   srvs,
		option:                    defaultOpts,
		subscribedTopic:           make(map[string]string, 0),
		done:                      make(chan struct{}, 1),
	}
	hpc.option.ClientOptions.Namesrv, err = internal.GetNamesrv(hpc.client.ClientID())
	if err != nil {
		return nil, err
	}
	hpc.namesrv = hpc.option.ClientOptions.Namesrv
	hpc.interceptor = primitive.ChainInterceptors(hpc.option.Interceptors...)

	if hpc.model == Clustering {
		retryTopic := internal.GetRetryTopic(hpc.consumerGroup)
		sub := buildSubscriptionData(retryTopic, MessageSelector{TAG, _SubAll})
		hpc.subscriptionDataTable.Store(retryTopic, sub)
	}
	return hpc, nil
}

func (hpc *defaultHighLevelPullConsumer) Start() error {
	atomic.StoreInt32(&hpc.state, int32(internal.StateRunning))

	var err error
	hpc.once.Do(func() {
		rlog.Info("the consumer start beginning", map[string]interface{}{
			rlog.LogKeyConsumerGroup: hpc.consumerGroup,
			"messageModel":           hpc.model,
			"unitMode":               hpc.unitMode,
		})
		atomic.StoreInt32(&hpc.state, int32(internal.StateStartFailed))
		hpc.validate()

		err = hpc.RegisterConsumer(hpc.consumerGroup, hpc)
		if err != nil {
			rlog.Error("the consumer group has been created, specify another one", map[string]interface{}{
				rlog.LogKeyConsumerGroup: hpc.consumerGroup,
			})
			err = errors2.ErrCreated
			return
		}

		if hpc.model == Clustering {
			// set retry topic
			retryTopic := internal.GetRetryTopic(hpc.consumerGroup)
			sub := buildSubscriptionData(retryTopic, MessageSelector{TAG, _SubAll})
			hpc.subscriptionDataTable.Store(retryTopic, sub)
		}

		if hpc.model == Clustering {
			hpc.option.ChangeInstanceNameToPID()
			hpc.storage = NewRemoteOffsetStore(hpc.consumerGroup, hpc.client, hpc.namesrv)
		} else {
			hpc.storage = NewLocalFileOffsetStore(hpc.consumerGroup, hpc.client.ClientID())
		}

		hpc.client.Start()
		atomic.StoreInt32(&hpc.state, int32(internal.StateRunning))
		hpc.consumerStartTimestamp = time.Now().UnixNano() / int64(time.Millisecond)
		if err != nil {
			return
		}
	})
	if err != nil {
		return err
	}

	hpc.client.UpdateTopicRouteInfo()
	for k := range hpc.subscribedTopic {
		_, exist := hpc.topicSubscribeInfoTable.Load(k)
		if !exist {
			hpc.client.Shutdown()
			return fmt.Errorf("the topic=%s route info not found, it may not exist", k)
		}
	}
	hpc.client.SendHeartbeatToAllBrokerWithLock()
	hpc.Rebalance()
	return err
}

func (hpc *defaultHighLevelPullConsumer) Shutdown() error {
	var err error
	hpc.closeOnce.Do(func() {
		close(hpc.done)
		hpc.UnregisterConsumer(hpc.consumerGroup)
		atomic.StoreInt32(&hpc.state, int32(internal.StateShutdown))
		mqs := make([]*primitive.MessageQueue, 0)
		hpc.processQueueTable.Range(func(key, value interface{}) bool {
			k := key.(primitive.MessageQueue)
			pq := value.(*processQueue)
			pq.WithDropped(true)
			// close msg channel using RWMutex to make sure no data was writing
			pq.mutex.Lock()
			close(pq.msgCh)
			pq.mutex.Unlock()
			mqs = append(mqs, &k)
			return true
		})
		hpc.storage.persist(mqs)
		hpc.client.Shutdown()
	})
	return err
}

func (hpc *defaultHighLevelPullConsumer) Subscribe(topic string, selector MessageSelector) error {
	if atomic.LoadInt32(&hpc.state) == int32(internal.StateStartFailed) ||
		atomic.LoadInt32(&hpc.state) == int32(internal.StateShutdown) {
		return errors2.ErrStartTopic
	}

	if "" == topic {
		rlog.Error(fmt.Sprintf("topic is null"), nil)
	}

	if hpc.option.Namespace != "" {
		topic = hpc.option.Namespace + "%" + topic
	}
	data := buildSubscriptionData(topic, selector)
	hpc.subscriptionDataTable.Store(topic, data)
	hpc.subscribedTopic[topic] = ""
	routeData , _ , err := hpc.namesrv.UpdateTopicRouteInfo(topic)
	if err == nil{
		hpc.topicSubscribeInfoTable.Store(topic , routeData)
	}
	hpc.Rebalance()
	return nil
}

func (hpc *defaultHighLevelPullConsumer) Unsubscribe(topic string) error {
	if hpc.option.Namespace != "" {
		topic = hpc.option.Namespace + "%" + topic
	}

	if "" == topic {
		rlog.Error(fmt.Sprintf("topic is null"), nil)
	}

	hpc.subscriptionDataTable.Delete(topic)
	return nil
}

func (hpc *defaultHighLevelPullConsumer) Pull(ctx context.Context, topic string, numbers int) (*primitive.PullResult, error) {
	queue, err := hpc.getNextQueueOf(topic)
	if err != nil {
		return nil, err
	}

	if queue == nil {
		return nil, fmt.Errorf("prepard to pull topic: %s, but no queue is founded", topic)
	}
	subData := buildSubscriptionData(queue.Topic, MessageSelector{
		Expression: _SubAll,
	})
	result, _ := hpc.PullFromQueue(ctx, hpc.consumerGroup, queue, hpc.nextOffsetOf(queue), numbers)
	pq := newProcessQueue(false)
	hpc.processQueueTable.Store(*queue, pq)
	hpc.processPullResult(queue, result, subData)
	hpc.UpdateOffset(queue, result.NextBeginOffset)
	return result, err
}

func (hpc *defaultHighLevelPullConsumer) getNextQueueOf(topic string) (*primitive.MessageQueue, error) {
	queues, err := hpc.namesrv.FetchSubscribeMessageQueues(topic)
	if err != nil || len(queues) == 0 {
		return nil, fmt.Errorf("topic not exist: %s", topic)
	}
	var index int64
	v, exist := queueCounterTable.Load(topic)
	if !exist {
		index = 0
		queueCounterTable.Store(topic, int64(1))
	} else {
		index = v.(int64)
		queueCounterTable.Store(topic, index+int64(1))
	}

	return queues[int(index)%len(queues)], err
}

func (hpc *defaultHighLevelPullConsumer) nextOffsetOf(queue *primitive.MessageQueue) int64 {
	return hpc.computePullFromWhere(queue)
}

func (hpc *defaultHighLevelPullConsumer) Commit(ctx context.Context, topic string) {
	queues, err := hpc.namesrv.FetchSubscribeMessageQueues(topic)
	if err != nil {
		rlog.Error(fmt.Sprintf("fectch messageQueues error"), nil)
	}
	hpc.storage.persist(queues)
}

func (hpc *defaultHighLevelPullConsumer) validate() {
	internal.ValidateGroup(hpc.consumerGroup)

	if hpc.consumerGroup == internal.DefaultConsumerGroup {
		rlog.Error(fmt.Sprintf("consumerGroup can't equal [%s], please specify another one.", internal.DefaultConsumerGroup), nil)
	}

	if hpc.allocate == nil {
		rlog.Error(fmt.Sprintf("allocateMessageQueueStrategy is null"), nil)
	}

}

func (hpc *defaultHighLevelPullConsumer) computePullFromWhere(mq *primitive.MessageQueue) int64 {
	var result = int64(-1)
	var lastOffset = int64(-1)
	lastOffset = hpc.storage.read(mq, _ReadMemoryThenStore)

	if lastOffset >= 0 {
		result = lastOffset
	} else {
		switch hpc.option.FromWhere {
		case ConsumeFromLastOffset:
			if lastOffset == -1 {
				if strings.HasPrefix(mq.Topic, internal.RetryGroupTopicPrefix) {
					result = 0
				} else {
					lastOffset, err := hpc.queryMaxOffset(context.Background(), mq)
					if err == nil {
						result = lastOffset
					} else {
						rlog.Warning("query max offset error", map[string]interface{}{
							rlog.LogKeyMessageQueue:  mq,
							rlog.LogKeyUnderlayError: err,
						})
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
				if strings.HasPrefix(mq.Topic, internal.RetryGroupTopicPrefix) {
					lastOffset, err := hpc.queryMaxOffset(context.Background(), mq)
					if err == nil {
						result = lastOffset
					} else {
						result = -1
						rlog.Warning("query max offset error", map[string]interface{}{
							rlog.LogKeyMessageQueue:  mq,
							rlog.LogKeyUnderlayError: err,
						})
					}
				} else {
					t, err := time.Parse("20060102150405", hpc.option.ConsumeTimestamp)
					if err != nil {
						result = -1
					} else {
						lastOffset, err := hpc.Lookup(context.Background(), mq, t.Unix()*1000)
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

func (hpc *defaultHighLevelPullConsumer) UpdateOffset(queue *primitive.MessageQueue, offset int64) error {
	hpc.storage.update(queue, offset, false)
	return nil
}

func (hpc *defaultHighLevelPullConsumer) UpdateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue) {
	_, exist := hpc.subscriptionDataTable.Load(topic)
	if exist {
		hpc.topicSubscribeInfoTable.Store(topic, mqs)
	}
}

func (hpc *defaultHighLevelPullConsumer) IsSubscribeTopicNeedUpdate(topic string) bool {
	_, exist := hpc.subscriptionDataTable.Load(topic)
	if !exist {
		return false
	}
	_, exist = hpc.topicSubscribeInfoTable.Load(topic)
	return !exist
}

func (hpc *defaultHighLevelPullConsumer) Rebalance() {
	//TODO not implemented yet
}

func (hpc *defaultHighLevelPullConsumer) IsUnitMode() bool {
	return hpc.unitMode
}
func (hpc *defaultHighLevelPullConsumer) PersistConsumerOffset() error {
	err := hpc.makeSureStateOK()
	if err != nil {
		return err
	}
	mqs := make([]*primitive.MessageQueue, 0)
	hpc.processQueueTable.Range(func(key, value interface{}) bool {
		k := key.(primitive.MessageQueue)
		mqs = append(mqs, &k)
		return true
	})
	hpc.storage.persist(mqs)
	return nil
}
func (hpc *defaultHighLevelPullConsumer) subscriptionAutomatically(topic string) {
	_, exist := hpc.subscriptionDataTable.Load(topic)
	if !exist {
		s := MessageSelector{
			Expression: _SubAll,
		}
		hpc.subscriptionDataTable.Store(topic, buildSubscriptionData(topic, s))
	}
}

func (hpc *defaultHighLevelPullConsumer) makeSureStateOK() error {
	if atomic.LoadInt32(&hpc.state) != int32(internal.StateRunning) {
		return fmt.Errorf("state not running, actually: %v", hpc.state)
	}
	return nil
}
func (hpc *defaultHighLevelPullConsumer) GetConsumerRunningInfo() *internal.ConsumerRunningInfo {
	info := internal.NewConsumerRunningInfo()

	hpc.subscriptionDataTable.Range(func(key, value interface{}) bool {
		topic := key.(string)
		info.SubscriptionData[value.(*internal.SubscriptionData)] = true
		status := internal.ConsumeStatus{
			PullRT:            hpc.stat.getPullRT(topic, hpc.consumerGroup).avgpt,
			PullTPS:           hpc.stat.getPullTPS(topic, hpc.consumerGroup).tps,
			ConsumeRT:         hpc.stat.getConsumeRT(topic, hpc.consumerGroup).avgpt,
			ConsumeOKTPS:      hpc.stat.getConsumeOKTPS(topic, hpc.consumerGroup).tps,
			ConsumeFailedTPS:  hpc.stat.getConsumeFailedTPS(topic, hpc.consumerGroup).tps,
			ConsumeFailedMsgs: hpc.stat.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + hpc.consumerGroup).sum,
		}
		info.StatusTable[topic] = status
		return true
	})
	return info
}

func (hpc *defaultHighLevelPullConsumer) GetcType() string {
	return string(hpc.cType)
}

func (hpc *defaultHighLevelPullConsumer) GetModel() string {
	return string(hpc.cType)
}
func (hpc *defaultHighLevelPullConsumer) SubscriptionDataList() []*internal.SubscriptionData {
	result := make([]*internal.SubscriptionData, 0)
	hpc.subscriptionDataTable.Range(func(key, value interface{}) bool {
		result = append(result, value.(*internal.SubscriptionData))
		return true
	})
	return result
}

func (hpc *defaultHighLevelPullConsumer) GetWhere() string {
	switch hpc.fromWhere {
	case ConsumeFromLastOffset:
		return "CONSUME_FROM_LAST_OFFSET"
	case ConsumeFromFirstOffset:
		return "CONSUME_FROM_FIRST_OFFSET"
	case ConsumeFromTimestamp:
		return "CONSUME_FROM_TIMESTAMP"
	default:
		return "UNKOWN"
	}
}

func (hpc *defaultHighLevelPullConsumer) RegisterConsumer(group string, consumer *defaultHighLevelPullConsumer) error {
	_, exist := hpc.consumerMap.Load(group)
	if exist {
		rlog.Warning("the consumer group exist already", map[string]interface{}{
			rlog.LogKeyConsumerGroup: group,
		})
		return fmt.Errorf("the consumer group exist already")
	}
	hpc.consumerMap.Store(group, consumer)
	return nil
}

func (hpc *defaultHighLevelPullConsumer) UnregisterConsumer(group string) {
	hpc.consumerMap.Delete(group)
}
