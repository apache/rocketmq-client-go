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
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

type PullConsumer interface {
	// Start
	Start() error

	Subscribe(topic string, selector MessageSelector) error

	// Shutdown refuse all new pull operation, finish all submitted.
	Shutdown() error

	// Pull pull message of topic,  selector indicate which queue to pull.
	Pull(ctx context.Context, numbers int) (*primitive.PullResult, error)

	// PullFrom pull messages of queue from the offset to offset + numbers
	PullFrom(ctx context.Context, queue *primitive.MessageQueue, offset int64, numbers int) (*primitive.PullResult, error)

	// updateOffset update offset of queue in mem
	UpdateOffset(queue *primitive.MessageQueue, offset int64) error

	// PersistOffset persist all offset in mem.
	PersistOffset(ctx context.Context, topic string) error

	// CurrentOffset return the current offset of queue in mem.
	CurrentOffset(queue *primitive.MessageQueue) (int64, error)
}

type defaultPullConsumer struct {
	*defaultConsumer

	option            consumerOptions
	topic             string
	selector          MessageSelector
	GroupName         string
	Model             MessageModel
	UnitMode          bool
	nextQueueSequence int64
	allocateQueues    []*primitive.MessageQueue

	interceptor primitive.Interceptor
}

func NewPullConsumer(options ...Option) (PullConsumer, error) {
	defaultOpts := defaultPullConsumerOptions()
	for _, apply := range options {
		apply(&defaultOpts)
	}

	srvs, err := internal.NewNamesrv(defaultOpts.Resolver, defaultOpts.RemotingClientConfig)
	if err != nil {
		return nil, errors.Wrap(err, "new Namesrv failed.")
	}

	defaultOpts.Namesrv = srvs
	dc := &defaultConsumer{
		client:        internal.GetOrNewRocketMQClient(defaultOpts.ClientOptions, nil),
		consumerGroup: utils.WrapNamespace(defaultOpts.Namespace, defaultOpts.GroupName),
		cType:         _PullConsume,
		state:         int32(internal.StateCreateJust),
		prCh:          make(chan PullRequest, 4),
		model:         defaultOpts.ConsumerModel,
		option:        defaultOpts,
		allocate:      defaultOpts.Strategy,
	}
	if dc.client == nil {
		return nil, fmt.Errorf("GetOrNewRocketMQClient faild")
	}
	defaultOpts.Namesrv = dc.client.GetNameSrv()

	c := &defaultPullConsumer{
		defaultConsumer: dc,
	}
	dc.mqChanged = c.messageQueueChanged
	return c, nil
}

func (pc *defaultPullConsumer) Subscribe(topic string, selector MessageSelector) error {
	if atomic.LoadInt32(&pc.state) == int32(internal.StateStartFailed) ||
		atomic.LoadInt32(&pc.state) == int32(internal.StateShutdown) {
		return errors2.ErrStartTopic
	}
	topic = utils.WrapNamespace(pc.defaultConsumer.option.Namespace, topic)

	data := buildSubscriptionData(topic, selector)
	pc.subscriptionDataTable.Store(topic, data)
	pc.topic = topic
	pc.selector = selector

	return nil
}

func (pc *defaultPullConsumer) Start() error {
	atomic.StoreInt32(&pc.state, int32(internal.StateRunning))

	var err error
	pc.once.Do(func() {
		consumerGroupWithNs := utils.WrapNamespace(pc.defaultConsumer.option.Namespace, pc.consumerGroup)
		err = pc.defaultConsumer.client.RegisterConsumer(consumerGroupWithNs, pc)
		if err != nil {
			rlog.Error("defaultPullConsumer the consumer group has been created, specify another one", map[string]interface{}{
				rlog.LogKeyConsumerGroup: pc.consumerGroup,
			})
			err = errors2.ErrCreated
			return
		}
		err = pc.start()
		if err != nil {
			return
		}
	})

	pc.client.UpdateTopicRouteInfo()
	_, exist := pc.topicSubscribeInfoTable.Load(pc.topic)
	if !exist {
		err = pc.Shutdown()
		if err != nil {
			rlog.Error("defaultPullConsumer.Shutdown . route info not found, it may not exist", map[string]interface{}{
				rlog.LogKeyTopic:         pc.topic,
				rlog.LogKeyUnderlayError: err,
			})
		}
		return fmt.Errorf("the topic=%s route info not found, it may not exist", pc.topic)
	}
	pc.client.CheckClientInBroker()
	pc.client.SendHeartbeatToAllBrokerWithLock()
	pc.client.RebalanceImmediately()

	return err
}

func (pc *defaultPullConsumer) Pull(ctx context.Context, numbers int) (*primitive.PullResult, error) {
	mq := pc.getNextQueueOf(pc.topic)
	if mq == nil {
		return nil, fmt.Errorf("prepard to pull topic: %s, but no queue is founded", pc.topic)
	}

	data := buildSubscriptionData(mq.Topic, pc.selector)
	result, err := pc.pull(context.Background(), mq, data, pc.nextOffsetOf(mq), numbers)

	if err != nil {
		return nil, err
	}

	pc.processPullResult(mq, result, data)
	return result, nil
}

func (pc *defaultPullConsumer) getNextQueueOf(topic string) *primitive.MessageQueue {
	var queues []*primitive.MessageQueue
	var err error
	if len(pc.allocateQueues) == 0 {
		topic = utils.WrapNamespace(pc.defaultConsumer.option.Namespace, topic)
		queues, err = pc.defaultConsumer.client.GetNameSrv().FetchSubscribeMessageQueues(topic)
		if err != nil {
			rlog.Error("get next mq error", map[string]interface{}{
				rlog.LogKeyTopic:         topic,
				rlog.LogKeyUnderlayError: err.Error(),
			})
			return nil
		}

		if len(queues) == 0 {
			rlog.Warning("defaultPullConsumer.getNextQueueOf len is 0", map[string]interface{}{
				rlog.LogKeyTopic: topic,
			})
			return nil
		}
	} else {
		queues = pc.allocateQueues
	}
	index := int(atomic.LoadInt64(&pc.nextQueueSequence)) % len(queues)
	atomic.AddInt64(&pc.nextQueueSequence, 1)

	nextQueue := queues[index]
	rlog.Info("defaultPullConsumer.getNextQueueOf", map[string]interface{}{
		rlog.LogKeyTopic:                topic,
		rlog.LogKeyConsumerGroup:        pc.consumerGroup,
		rlog.LogKeyMessageQueue:         queues,
		rlog.LogKeyAllocateMessageQueue: nextQueue.String(),
	})

	return nextQueue
}

func (pc *defaultPullConsumer) checkPull(ctx context.Context, mq *primitive.MessageQueue, offset int64, numbers int) error {
	err := pc.makeSureStateOK()
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

// TODO: add timeout limit
// TODO: add hook
func (pc *defaultPullConsumer) pull(ctx context.Context, mq *primitive.MessageQueue, data *internal.SubscriptionData,
	offset int64, numbers int) (*primitive.PullResult, error) {

	mq.Topic = utils.WrapNamespace(pc.defaultConsumer.option.Namespace, mq.Topic)
	pc.consumerGroup = utils.WrapNamespace(pc.defaultConsumer.option.Namespace, pc.consumerGroup)

	if err := pc.checkPull(ctx, mq, offset, numbers); err != nil {
		return nil, err
	}

	pc.subscriptionAutomatically(mq.Topic)

	sysFlag := buildSysFlag(false, true, true, false)

	pullResp, err := pc.pullInner(ctx, mq, data, offset, numbers, sysFlag, 0)
	if err != nil {
		return nil, err
	}
	pc.processPullResult(mq, pullResp, data)

	return pullResp, err
}

func (pc *defaultPullConsumer) nextOffsetOf(queue *primitive.MessageQueue) int64 {
	result, _ := pc.computePullFromWhereWithException(queue)
	return result
}

// PullFrom pull messages of queue from the offset to offset + numbers
func (pc *defaultPullConsumer) PullFrom(ctx context.Context, queue *primitive.MessageQueue, offset int64, numbers int) (*primitive.PullResult, error) {
	if err := pc.checkPull(ctx, queue, offset, numbers); err != nil {
		return nil, err
	}

	data := buildSubscriptionData(queue.Topic, pc.selector)

	return pc.pull(ctx, queue, data, offset, numbers)
}

// updateOffset update offset of queue in mem
func (pc *defaultPullConsumer) UpdateOffset(queue *primitive.MessageQueue, offset int64) error {
	return pc.updateOffset(queue, offset)
}

// PersistOffset persist all offset in mem.
func (pc *defaultPullConsumer) PersistOffset(ctx context.Context, topic string) error {

	if len(pc.allocateQueues) == 0 {
		rlog.Warning("defaultPullConsumer.PersistOffset len is 0", map[string]interface{}{
			rlog.LogKeyTopic: topic,
		})
		return nil
	}
	return pc.persistPullConsumerOffset(pc.allocateQueues)
}

// CurrentOffset return the current offset of queue in mem.
func (pc *defaultPullConsumer) CurrentOffset(queue *primitive.MessageQueue) (int64, error) {
	v := pc.queryOffset(queue)
	return v, nil
}

// Shutdown close defaultConsumer, refuse new request.
func (pc *defaultPullConsumer) Shutdown() error {
	return pc.defaultConsumer.shutdown()
}

func (pc *defaultPullConsumer) PersistConsumerOffset() error {
	return pc.defaultConsumer.persistConsumerOffset()
}

func (pc *defaultPullConsumer) UpdateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue) {
	pc.defaultConsumer.updateTopicSubscribeInfo(topic, mqs)
}

func (pc *defaultPullConsumer) IsSubscribeTopicNeedUpdate(topic string) bool {
	return pc.defaultConsumer.isSubscribeTopicNeedUpdate(topic)
}

func (pc *defaultPullConsumer) SubscriptionDataList() []*internal.SubscriptionData {
	return pc.defaultConsumer.SubscriptionDataList()
}

func (pc *defaultPullConsumer) IsUnitMode() bool {
	return pc.unitMode
}

func (pc *defaultPullConsumer) GetcType() string {
	return string(pc.cType)
}

func (pc *defaultPullConsumer) GetModel() string {
	return pc.model.String()
}

func (pc *defaultPullConsumer) GetWhere() string {
	switch pc.fromWhere {
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

func (pc *defaultPullConsumer) Rebalance() {
	pc.defaultConsumer.doBalance()
}

func (pc *defaultPullConsumer) RebalanceIfNotPaused() {
	pc.defaultConsumer.doBalanceIfNotPaused()
}

func (pc *defaultPullConsumer) GetConsumerRunningInfo(stack bool) *internal.ConsumerRunningInfo {
	info := internal.NewConsumerRunningInfo()

	pc.subscriptionDataTable.Range(func(key, value interface{}) bool {
		topic := key.(string)
		info.SubscriptionData[value.(*internal.SubscriptionData)] = true
		status := internal.ConsumeStatus{
			PullRT:            pc.stat.getPullRT(pc.consumerGroup, topic).avgpt,
			PullTPS:           pc.stat.getPullTPS(pc.consumerGroup, topic).tps,
			ConsumeRT:         pc.stat.getConsumeRT(pc.consumerGroup, topic).avgpt,
			ConsumeOKTPS:      pc.stat.getConsumeOKTPS(pc.consumerGroup, topic).tps,
			ConsumeFailedTPS:  pc.stat.getConsumeFailedTPS(pc.consumerGroup, topic).tps,
			ConsumeFailedMsgs: pc.stat.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + pc.consumerGroup).sum,
		}
		info.StatusTable[topic] = status
		return true
	})

	pc.processQueueTable.Range(func(key, value interface{}) bool {
		mq := key.(primitive.MessageQueue)
		pq := value.(*processQueue)
		pInfo := pq.currentInfo()
		pInfo.CommitOffset, _ = pc.storage.readWithException(&mq, _ReadMemoryThenStore)
		info.MQTable[mq] = pInfo
		return true
	})

	if stack {
		var buffer strings.Builder

		err := pprof.Lookup("goroutine").WriteTo(&buffer, 2)
		if err != nil {
			rlog.Error("error when get stack ", map[string]interface{}{
				"error": err,
			})
		} else {
			info.JStack = buffer.String()
		}
	}

	nsAddr := ""
	for _, value := range pc.client.GetNameSrv().AddrList() {
		nsAddr += fmt.Sprintf("%s;", value)
	}
	info.Properties[internal.PropNameServerAddr] = nsAddr
	info.Properties[internal.PropConsumeType] = string(pc.cType)
	info.Properties[internal.PropConsumeOrderly] = strconv.FormatBool(pc.consumeOrderly)
	info.Properties[internal.PropThreadPoolCoreSize] = "-1"
	info.Properties[internal.PropConsumerStartTimestamp] = strconv.FormatInt(pc.consumerStartTimestamp, 10)
	return info
}

func (pc *defaultPullConsumer) ConsumeMessageDirectly(msg *primitive.MessageExt, brokerName string) *internal.ConsumeMessageDirectlyResult {
	return nil
}

func (pc *defaultPullConsumer) ResetOffset(topic string, table map[primitive.MessageQueue]int64) {

}

func (pc *defaultPullConsumer) messageQueueChanged(topic string, mqAll, mqDivided []*primitive.MessageQueue) {
	var allocateQueues []*primitive.MessageQueue
	pc.defaultConsumer.processQueueTable.Range(func(key, value interface{}) bool {
		mq := key.(primitive.MessageQueue)
		allocateQueues = append(allocateQueues, &mq)
		return true
	})
	pc.allocateQueues = allocateQueues
	pc.defaultConsumer.client.SendHeartbeatToAllBrokerWithLock()
}

func (pc *defaultPullConsumer) sendMessageBack(brokerName string, msg *primitive.MessageExt, delayLevel int) bool {
	var brokerAddr string
	if len(brokerName) != 0 {
		brokerAddr = pc.defaultConsumer.client.GetNameSrv().FindBrokerAddrByName(brokerName)
	} else {
		brokerAddr = msg.StoreHost
	}
	_, err := pc.client.InvokeSync(context.Background(), brokerAddr, pc.buildSendBackRequest(msg, delayLevel), 3*time.Second)
	if err != nil {
		return false
	}
	return true
}

func (pc *defaultPullConsumer) buildSendBackRequest(msg *primitive.MessageExt, delayLevel int) *remote.RemotingCommand {
	req := &internal.ConsumerSendMsgBackRequestHeader{
		Group:             pc.consumerGroup,
		OriginTopic:       msg.Topic,
		Offset:            msg.CommitLogOffset,
		DelayLevel:        delayLevel,
		OriginMsgId:       msg.MsgId,
		MaxReconsumeTimes: pc.getMaxReconsumeTimes(),
	}

	return remote.NewRemotingCommand(internal.ReqConsumerSendMsgBack, req, nil)
}

func (pc *defaultPullConsumer) getMaxReconsumeTimes() int32 {
	if pc.option.MaxReconsumeTimes == -1 {
		return 16
	} else {
		return pc.option.MaxReconsumeTimes
	}
}
