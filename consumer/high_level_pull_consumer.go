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
	"sync"
	"sync/atomic"
	"time"
)

type highLevelPullConsumer struct {
	*defaultConsumer

	option   consumerOptions
	Model    MessageModel
	UnitMode bool

	subscribedTopic       map[string]string
	closeOnce             sync.Once
	interceptor           primitive.Interceptor
	done                  chan struct{}
	queueFlowControlTimes int
}

func NewHighLevelPullConsumer(options ...Option) (*highLevelPullConsumer, error) {
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

	dc := &defaultConsumer{
		client:        internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil),
		consumerGroup: defaultOpts.GroupName,
		cType:         _PullConsume,
		state:         int32(internal.StateCreateJust),
		model:         defaultOpts.ConsumerModel,
		allocate:      defaultOpts.Strategy,
		namesrv:       srvs,
		option:        defaultOpts,
	}
	dc.option.ClientOptions.Namesrv, err = internal.GetNamesrv(dc.client.ClientID())
	if err != nil {
		return nil, err
	}
	dc.namesrv = dc.option.ClientOptions.Namesrv
	hpc := &highLevelPullConsumer{
		defaultConsumer: dc,
		subscribedTopic: make(map[string]string, 0),
		done:            make(chan struct{}, 1),
	}
	dc.mqChanged = hpc.messageQueueChanged
	hpc.interceptor = primitive.ChainInterceptors(hpc.option.Interceptors...)

	if hpc.model == Clustering {
		retryTopic := internal.GetRetryTopic(hpc.consumerGroup)
		sub := buildSubscriptionData(retryTopic, MessageSelector{TAG, _SubAll})
		hpc.subscriptionDataTable.Store(retryTopic, sub)
	}
	return hpc, nil
}

func (hpc *highLevelPullConsumer) Start() error {
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

		err = hpc.client.RegisterConsumer(hpc.consumerGroup, hpc)
		if err != nil {
			rlog.Error("the consumer group has been created, specify another one", map[string]interface{}{
				rlog.LogKeyConsumerGroup: hpc.consumerGroup,
			})
			err = errors2.ErrCreated
			return
		}

		err = hpc.defaultConsumer.start()
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

	hpc.client.CheckClientInBroker()
	hpc.client.SendHeartbeatToAllBrokerWithLock()
	hpc.client.RebalanceImmediately()

	return err
}

func (hpc *highLevelPullConsumer) Shutdown() error {
	var err error
	fmt.Println("程序结束")
	hpc.closeOnce.Do(func() {
		close(hpc.done)
		hpc.client.UnregisterConsumer(hpc.consumerGroup)
		err = hpc.defaultConsumer.shutdown()
	})
	return err
}

func (hpc *highLevelPullConsumer) Subscribe(topic string, selector MessageSelector) error {
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
	hpc.namesrv.UpdateTopicRouteInfo(topic)
	hpc.client.RebalanceImmediately()
	return nil
}

func (hpc *highLevelPullConsumer) Unsubscribe(topic string) error {
	if hpc.option.Namespace != "" {
		topic = hpc.option.Namespace + "%" + topic
	}

	if "" == topic {
		rlog.Error(fmt.Sprintf("topic is null"), nil)
	}

	hpc.subscriptionDataTable.Delete(topic)
	return nil
}

func (hpc *highLevelPullConsumer) Pull(ctx context.Context, topic string, numbers int) (*primitive.PullResult, error) {
	queue, err := hpc.getNextQueueOf(topic)
	if err != nil {
		return nil, err
	}

	if queue == nil {
		return nil, fmt.Errorf("prepard to pull topic: %s, but no queue is founded", topic)
	}

	v, _ := hpc.subscriptionDataTable.Load(topic)
	if v == nil {
		return nil, fmt.Errorf("this consumer not subscribe the topic: %s", topic)
	}
	subData := v.(*internal.SubscriptionData)
	data := buildSubscriptionData(queue.Topic, MessageSelector{
		Expression: subData.SubString,
		Type:       ExpressionType(subData.ExpType),
	})

	result, _ := hpc.pull(context.Background(), queue, data, hpc.nextOffsetOf(queue), numbers)
	pq := newProcessQueue(false)
	hpc.processQueueTable.Store(*queue, pq)
	hpc.processPullResult(queue, result, data)
	hpc.UpdateOffset(queue, result.NextBeginOffset)
	return result, err
}

func (hpc *highLevelPullConsumer) getNextQueueOf(topic string) (*primitive.MessageQueue, error) {
	queues, err := hpc.defaultConsumer.namesrv.FetchSubscribeMessageQueues(topic)
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

func (hpc *highLevelPullConsumer) GetMessageQueues(topic string) ([]*primitive.MessageQueue, error) {
	queues, err := hpc.namesrv.FetchSubscribeMessageQueues(topic)
	if err != nil || len(queues) == 0 {
		return nil, fmt.Errorf("topic not exist: %s", topic)
	}
	return queues, err
}

func (hpc *highLevelPullConsumer) nextOffsetOf(queue *primitive.MessageQueue) int64 {
	return hpc.computePullFromWhere(queue)
}

func (hpc *highLevelPullConsumer) Commit(ctx context.Context, topic string) {
	queues, err := hpc.defaultConsumer.namesrv.FetchSubscribeMessageQueues(topic)
	if err != nil {
		rlog.Error(fmt.Sprintf("fectch messageQueues error"), nil)
	}
	hpc.storage.persist(queues)
}

func (hpc *highLevelPullConsumer) validate() {
	internal.ValidateGroup(hpc.consumerGroup)

	if hpc.consumerGroup == internal.DefaultConsumerGroup {
		rlog.Error(fmt.Sprintf("consumerGroup can't equal [%s], please specify another one.", internal.DefaultConsumerGroup), nil)
	}

	if hpc.allocate == nil {
		rlog.Error(fmt.Sprintf("allocateMessageQueueStrategy is null"), nil)
	}

}

func (hpc *highLevelPullConsumer) pull(ctx context.Context, mq *primitive.MessageQueue, data *internal.SubscriptionData,
	offset int64, numbers int) (*primitive.PullResult, error) {

	if err := hpc.checkPull(ctx, mq, offset, numbers); err != nil {
		return nil, err
	}

	hpc.subscriptionAutomatically(mq.Topic)

	sysFlag := buildSysFlag(false, true, true, false)

	pullResp, err := hpc.pullInner(ctx, mq, data, offset, numbers, sysFlag, 0)

	if err != nil {
		return nil, err
	}

	hpc.processPullResult(mq, pullResp, data)

	return pullResp, err
}
func (hpc *highLevelPullConsumer) checkPull(ctx context.Context, mq *primitive.MessageQueue, offset int64, numbers int) error {
	err := hpc.makeSureStateOK()
	if err != nil {
		return err
	}

	if mq == nil {
		return errors2.ErrMQEmpty
	}

	if offset < 0 {
		return errors2.ErrOffset
	}

	if numbers <= 0 {
		return errors2.ErrNumbers
	}
	return nil
}

func (hpc *highLevelPullConsumer) messageQueueChanged(topic string, all []*primitive.MessageQueue, divided []*primitive.MessageQueue) {
	v, exit := hpc.subscriptionDataTable.Load(topic)
	if !exit {
		return
	}
	data := v.(*internal.SubscriptionData)
	newVersion := time.Now().UnixNano()
	rlog.Info("the MessageQueue changed, version also updated", map[string]interface{}{
		rlog.LogKeyValueChangedFrom: data.SubVersion,
		rlog.LogKeyValueChangedTo:   newVersion,
	})
	data.SubVersion = newVersion

	count := 0
	hpc.processQueueTable.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	if count > 0 {
		if hpc.option.PullThresholdForTopic != -1 {
			newVal := hpc.option.PullThresholdForTopic / count
			if newVal == 0 {
				newVal = 1
			}
			rlog.Info("The PullThresholdForTopic is changed", map[string]interface{}{
				rlog.LogKeyValueChangedFrom: hpc.option.PullThresholdForTopic,
				rlog.LogKeyValueChangedTo:   newVal,
			})
			hpc.option.PullThresholdForTopic = newVal
		}

		if hpc.option.PullThresholdSizeForTopic != -1 {
			newVal := hpc.option.PullThresholdSizeForTopic / count
			if newVal == 0 {
				newVal = 1
			}
			rlog.Info("The PullThresholdSizeForTopic is changed", map[string]interface{}{
				rlog.LogKeyValueChangedFrom: hpc.option.PullThresholdSizeForTopic,
				rlog.LogKeyValueChangedTo:   newVal,
			})
		}
	}
	hpc.client.SendHeartbeatToAllBrokerWithLock()
}
func (hpc *highLevelPullConsumer) UpdateOffset(queue *primitive.MessageQueue, offset int64) error {
	return hpc.updateOffset(queue, offset)
}

func (hpc *highLevelPullConsumer) UpdateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue) {
	hpc.defaultConsumer.updateTopicSubscribeInfo(topic, mqs)
}

func (hpc *highLevelPullConsumer) IsSubscribeTopicNeedUpdate(topic string) bool {
	return hpc.defaultConsumer.isSubscribeTopicNeedUpdate(topic)
}

func (hpc *highLevelPullConsumer) Rebalance() {
	hpc.defaultConsumer.doBalance()
}

func (hpc *highLevelPullConsumer) IsUnitMode() bool {
	return hpc.unitMode
}
func (hpc *highLevelPullConsumer) PersistConsumerOffset() error {
	return hpc.persistConsumerOffset()
}

func (hpc *highLevelPullConsumer) ConsumeMessageDirectly(msg *primitive.MessageExt, brokerName string) *internal.ConsumeMessageDirectlyResult {
	panic("implement me")
}

func (hpc *highLevelPullConsumer) GetConsumerRunningInfo() *internal.ConsumerRunningInfo {
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

func (hpc *highLevelPullConsumer) GetcType() string {
	return string(hpc.cType)
}

func (hpc *highLevelPullConsumer) GetModel() string {
	return string(hpc.cType)
}

func (hpc *highLevelPullConsumer) GetWhere() string {
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

func (hpc *highLevelPullConsumer) ResetOffset(topic string, table map[primitive.MessageQueue]int64) {
	panic("implement me")
}
