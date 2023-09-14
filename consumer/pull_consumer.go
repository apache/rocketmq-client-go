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
	"math"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	errors2 "github.com/apache/rocketmq-client-go/v2/errors"
	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/internal/utils"

	"github.com/pkg/errors"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
)

// ErrNoNewMsg returns a "no new message found". Occurs only when no new message found from broker
var ErrNoNewMsg = errors.New("no new message found")

func IsNoNewMsgError(err error) bool {
	return err == ErrNoNewMsg
}

type ConsumeRequest struct {
	messageQueue *primitive.MessageQueue
	processQueue *processQueue
	msgList      []*primitive.MessageExt
}

func (cr *ConsumeRequest) GetMsgList() []*primitive.MessageExt {
	return cr.msgList
}

func (cr *ConsumeRequest) GetMQ() *primitive.MessageQueue {
	return cr.messageQueue
}

func (cr *ConsumeRequest) GetPQ() *processQueue {
	return cr.processQueue
}

type defaultPullConsumer struct {
	*defaultConsumer

	topic             string
	selector          MessageSelector
	GroupName         string
	Model             MessageModel
	UnitMode          bool
	nextQueueSequence int64
	allocateQueues    []*primitive.MessageQueue

	done                chan struct{}
	closeOnce           sync.Once
	consumeRequestCache chan *ConsumeRequest
	submitToConsume     func(*processQueue, *primitive.MessageQueue)
	interceptor         primitive.Interceptor
}

func NewPullConsumer(options ...Option) (*defaultPullConsumer, error) {
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
		defaultConsumer:     dc,
		done:                make(chan struct{}, 1),
		consumeRequestCache: make(chan *ConsumeRequest, 4),
	}
	dc.mqChanged = c.messageQueueChanged
	c.submitToConsume = c.consumeMessageConcurrently
	c.interceptor = primitive.ChainInterceptors(c.option.Interceptors...)
	return c, nil
}

func (pc *defaultPullConsumer) Subscribe(topic string, selector MessageSelector) error {
	if atomic.LoadInt32(&pc.state) == int32(internal.StateStartFailed) ||
		atomic.LoadInt32(&pc.state) == int32(internal.StateShutdown) {
		return errors2.ErrStartTopic
	}
	topic = utils.WrapNamespace(pc.option.Namespace, topic)

	data := buildSubscriptionData(topic, selector)
	pc.subscriptionDataTable.Store(topic, data)
	pc.topic = topic
	pc.selector = selector

	return nil
}

func (pc *defaultPullConsumer) Unsubscribe(topic string) error {
	topic = utils.WrapNamespace(pc.option.Namespace, topic)
	pc.subscriptionDataTable.Delete(topic)
	return nil
}

func (pc *defaultPullConsumer) Start() error {
	var err error
	pc.once.Do(func() {
		err = pc.validate()
		if err != nil {
			rlog.Error("the consumer group option validate fail", map[string]interface{}{
				rlog.LogKeyConsumerGroup: pc.consumerGroup,
				rlog.LogKeyUnderlayError: err.Error(),
			})
			err = errors.Wrap(err, "the consumer group option validate fail")
			return
		}
		err = pc.defaultConsumer.client.RegisterConsumer(pc.consumerGroup, pc)
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
		atomic.StoreInt32(&pc.state, int32(internal.StateRunning))
		go func() {
			for {
				select {
				case pr := <-pc.prCh:
					go func() {
						pc.pullMessage(&pr)
					}()
				case <-pc.done:
					rlog.Info("defaultPullConsumer close PullRequest listener.", map[string]interface{}{
						rlog.LogKeyConsumerGroup: pc.consumerGroup,
					})
					return
				}
			}
		}()
	})
	if err != nil {
		return err
	}
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
	go pc.client.RebalanceImmediately()

	return err
}

func (pc *defaultPullConsumer) Poll(ctx context.Context, timeout time.Duration) (*ConsumeRequest, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, ErrNoNewMsg
	case cr := <-pc.consumeRequestCache:
		if cr.processQueue.IsDroppd() {
			rlog.Info("defaultPullConsumer poll the message queue not be able to consume, because it was dropped", map[string]interface{}{
				rlog.LogKeyMessageQueue:  cr.messageQueue.String(),
				rlog.LogKeyConsumerGroup: pc.consumerGroup,
			})
			return nil, ErrNoNewMsg
		}

		if len(cr.GetMsgList()) == 0 {
			return nil, ErrNoNewMsg
		}
		return cr, nil
	}
}

func (pc *defaultPullConsumer) ACK(ctx context.Context, cr *ConsumeRequest, result ConsumeResult) {
	if cr == nil {
		return
	}
	pq := cr.processQueue
	mq := cr.messageQueue
	msgList := cr.msgList
	if len(msgList) == 0 || pq == nil || mq == nil {
		return
	}
RETRY:
	if pq.IsDroppd() {
		rlog.Info("defaultPullConsumer the message queue not be able to consume, because it was dropped", map[string]interface{}{
			rlog.LogKeyMessageQueue:  mq.String(),
			rlog.LogKeyConsumerGroup: pc.consumerGroup,
		})
		return
	}

	pc.resetRetryAndNamespace(msgList)

	msgCtx := &primitive.ConsumeMessageContext{
		Properties:    make(map[string]string),
		ConsumerGroup: pc.consumerGroup,
		MQ:            mq,
		Msgs:          msgList,
	}
	ctx = primitive.WithConsumerCtx(ctx, msgCtx)
	ctx = primitive.WithMethod(ctx, primitive.ConsumerPull)
	concurrentCtx := primitive.NewConsumeConcurrentlyContext()
	concurrentCtx.MQ = *mq
	ctx = primitive.WithConcurrentlyCtx(ctx, concurrentCtx)

	if result == ConsumeSuccess {
		msgCtx.Properties[primitive.PropCtxType] = string(primitive.SuccessReturn)
		msgCtx.Success = true
	} else {
		msgCtx.Properties[primitive.PropCtxType] = string(primitive.FailedReturn)
		msgCtx.Success = false
	}

	if pc.interceptor != nil {
		pc.interceptor(ctx, msgList, nil, func(ctx context.Context, req, reply interface{}) error {
			return nil
		})
	}

	if !pq.IsDroppd() {
		msgBackFailed := make([]*primitive.MessageExt, 0)
		msgBackSucceed := make([]*primitive.MessageExt, 0)
		if result == ConsumeSuccess {
			pc.stat.increaseConsumeOKTPS(pc.consumerGroup, mq.Topic, len(msgList))
			msgBackSucceed = msgList
		} else {
			pc.stat.increaseConsumeFailedTPS(pc.consumerGroup, mq.Topic, len(msgList))
			if pc.model == BroadCasting {
				for i := 0; i < len(msgList); i++ {
					rlog.Warning("defaultPullConsumer BROADCASTING, the message consume failed, drop it", map[string]interface{}{
						"message": msgList[i],
					})
				}
			} else {
				for i := 0; i < len(msgList); i++ {
					msg := msgList[i]
					if pc.sendMessageBack(mq.BrokerName, msg, concurrentCtx.DelayLevelWhenNextConsume) {
						msgBackSucceed = append(msgBackSucceed, msg)
					} else {
						msg.ReconsumeTimes += 1
						msgBackFailed = append(msgBackFailed, msg)
					}
				}
			}
		}

		offset := pq.removeMessage(msgBackSucceed...)

		if offset >= 0 && !pq.IsDroppd() {
			pc.storage.update(mq, int64(offset), true)
		}
		if len(msgBackFailed) > 0 {
			msgList = msgBackFailed
			time.Sleep(5 * time.Second)
			goto RETRY
		}
	} else {
		rlog.Warning("defaultPullConsumer processQueue is dropped without process consume result.", map[string]interface{}{
			rlog.LogKeyMessageQueue: mq,
			"message":               msgList,
		})
	}

}

// resetRetryAndNamespace modify retry message.
func (pc *defaultPullConsumer) resetRetryAndNamespace(msgList []*primitive.MessageExt) {
	groupTopic := internal.RetryGroupTopicPrefix + pc.consumerGroup
	beginTime := time.Now()
	for idx := range msgList {
		msg := msgList[idx]
		retryTopic := msg.GetProperty(primitive.PropertyRetryTopic)
		if retryTopic == "" && groupTopic == msg.Topic {
			msg.Topic = retryTopic
		}
		msgList[idx].WithProperty(primitive.PropertyConsumeStartTime, strconv.FormatInt(
			beginTime.UnixNano()/int64(time.Millisecond), 10))
	}
}

func (pc *defaultPullConsumer) Pull(ctx context.Context, numbers int) (*primitive.PullResult, error) {
	mq := pc.getNextQueueOf(pc.topic)
	if mq == nil {
		return nil, fmt.Errorf("prepare to pull topic: %s, but no queue is founded", pc.topic)
	}

	data := buildSubscriptionData(mq.Topic, pc.selector)
	nextOffset, err := pc.nextOffsetOf(mq)
	if err != nil {
		return nil, err
	}

	result, err := pc.pull(context.Background(), mq, data, nextOffset, numbers)
	if err != nil {
		return nil, err
	}

	pc.processPullResult(mq, result, data)

	if pc.interceptor != nil {
		msgCtx := &primitive.ConsumeMessageContext{
			Properties:    make(map[string]string),
			ConsumerGroup: pc.consumerGroup,
			MQ:            mq,
			Msgs:          result.GetMessageExts(),
			Success:       true,
		}
		ctx = primitive.WithConsumerCtx(ctx, msgCtx)
		ctx = primitive.WithMethod(ctx, primitive.ConsumerPull)
		pc.interceptor(ctx, result.GetMessageExts(), nil, func(ctx context.Context, req, reply interface{}) error {
			return nil
		})
	}

	return result, nil
}

func (pc *defaultPullConsumer) getNextQueueOf(topic string) *primitive.MessageQueue {
	var queues []*primitive.MessageQueue
	var err error
	if len(pc.allocateQueues) == 0 {
		topic = utils.WrapNamespace(pc.option.Namespace, topic)
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

func (pc *defaultPullConsumer) checkPull(mq *primitive.MessageQueue, offset int64, numbers int) error {
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

	mq.Topic = utils.WrapNamespace(pc.option.Namespace, mq.Topic)
	pc.consumerGroup = utils.WrapNamespace(pc.option.Namespace, pc.consumerGroup)

	if err := pc.checkPull(mq, offset, numbers); err != nil {
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

func (pc *defaultPullConsumer) nextOffsetOf(queue *primitive.MessageQueue) (int64, error) {
	return pc.computePullFromWhereWithException(queue)
}

// PullFrom pull messages of queue from the offset to offset + numbers
func (pc *defaultPullConsumer) PullFrom(ctx context.Context, queue *primitive.MessageQueue, offset int64, numbers int) (*primitive.PullResult, error) {
	if err := pc.checkPull(queue, offset, numbers); err != nil {
		return nil, err
	}

	data := buildSubscriptionData(queue.Topic, pc.selector)

	return pc.pull(ctx, queue, data, offset, numbers)
}

// UpdateOffset updateOffset update offset of queue in mem
func (pc *defaultPullConsumer) UpdateOffset(queue *primitive.MessageQueue, offset int64) error {
	return pc.updateOffset(queue, offset)
}

// PersistOffset persist all offset in mem.
func (pc *defaultPullConsumer) PersistOffset(ctx context.Context, topic string) error {
	return pc.persistConsumerOffset()
}

// CurrentOffset return the current offset of queue in mem.
func (pc *defaultPullConsumer) CurrentOffset(queue *primitive.MessageQueue) (int64, error) {
	v := pc.queryOffset(queue)
	return v, nil
}

// Shutdown close defaultConsumer, refuse new request.
func (pc *defaultPullConsumer) Shutdown() error {
	var err error
	pc.closeOnce.Do(func() {
		if pc.option.TraceDispatcher != nil {
			pc.option.TraceDispatcher.Close()
		}
		close(pc.done)

		pc.client.UnregisterConsumer(pc.consumerGroup)
		err = pc.defaultConsumer.shutdown()
	})

	return err
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
		return "UNKNOWN"
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
	return err == nil
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

func (pc *defaultPullConsumer) pullMessage(request *PullRequest) {
	rlog.Debug("defaultPullConsumer start a new Pull Message task for PullRequest", map[string]interface{}{
		rlog.LogKeyPullRequest: request.String(),
	})
	var sleepTime time.Duration
	pq := request.pq
	go primitive.WithRecover(func() {
		for {
			select {
			case <-pc.done:
				rlog.Info("defaultPullConsumer close pullMessage.", map[string]interface{}{
					rlog.LogKeyConsumerGroup: pc.consumerGroup,
				})
				return
			default:
				pc.submitToConsume(request.pq, request.mq)
				if request.pq.IsDroppd() {
					rlog.Info("defaultPullConsumer quit pullMessage for dropped queue.", map[string]interface{}{
						rlog.LogKeyConsumerGroup: pc.consumerGroup,
					})
					return
				}
			}
		}
	})
	for {
	NEXT:
		select {
		case <-pc.done:
			rlog.Info("defaultPullConsumer close message handle.", map[string]interface{}{
				rlog.LogKeyConsumerGroup: pc.consumerGroup,
			})
			return
		default:
		}

		if pq.IsDroppd() {
			rlog.Debug("defaultPullConsumer the request was dropped, so stop task", map[string]interface{}{
				rlog.LogKeyPullRequest: request.String(),
			})
			return
		}
		if sleepTime > 0 {
			rlog.Debug(fmt.Sprintf("defaultPullConsumer pull MessageQueue: %d sleep %d ms for mq: %v", request.mq.QueueId, sleepTime/time.Millisecond, request.mq), nil)
			time.Sleep(sleepTime)
		}
		// reset time
		sleepTime = pc.option.PullInterval
		pq.lastPullTime.Store(time.Now())
		err := pc.makeSureStateOK()
		if err != nil {
			rlog.Warning("defaultPullConsumer state error", map[string]interface{}{
				rlog.LogKeyUnderlayError: err.Error(),
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		if pc.pause {
			rlog.Debug(fmt.Sprintf("defaultPullConsumer [%s] of [%s] was paused, execute pull request [%s] later",
				pc.option.InstanceName, pc.consumerGroup, request.String()), nil)
			sleepTime = _PullDelayTimeWhenSuspend
			goto NEXT
		}

		v, exist := pc.subscriptionDataTable.Load(request.mq.Topic)
		if !exist {
			rlog.Info("defaultPullConsumer find the consumer's subscription failed", map[string]interface{}{
				rlog.LogKeyPullRequest: request.String(),
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}
		beginTime := time.Now()
		sd := v.(*internal.SubscriptionData)

		sysFlag := buildSysFlag(false, true, true, false)

		pullRequest := &internal.PullMessageRequestHeader{
			ConsumerGroup:        pc.consumerGroup,
			Topic:                request.mq.Topic,
			QueueId:              int32(request.mq.QueueId),
			QueueOffset:          request.nextOffset,
			MaxMsgNums:           pc.option.PullBatchSize,
			SysFlag:              sysFlag,
			CommitOffset:         0,
			SubExpression:        sd.SubString,
			ExpressionType:       string(TAG),
			SuspendTimeoutMillis: 20 * time.Second,
		}

		brokerResult := pc.defaultConsumer.tryFindBroker(request.mq)
		if brokerResult == nil {
			rlog.Warning("defaultPullConsumer no broker found for mq", map[string]interface{}{
				rlog.LogKeyPullRequest: request.mq.String(),
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		if brokerResult.Slave {
			pullRequest.SysFlag = clearCommitOffsetFlag(pullRequest.SysFlag)
		}

		result, err := pc.client.PullMessage(context.Background(), brokerResult.BrokerAddr, pullRequest)
		if err != nil {
			rlog.Warning("defaultPullConsumer pull message from broker error", map[string]interface{}{
				rlog.LogKeyBroker:        brokerResult.BrokerAddr,
				rlog.LogKeyUnderlayError: err.Error(),
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		if result.Status == primitive.PullBrokerTimeout {
			rlog.Warning("defaultPullConsumer pull broker timeout", map[string]interface{}{
				rlog.LogKeyBroker: brokerResult.BrokerAddr,
			})
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		pc.processPullResult(request.mq, result, sd)

		switch result.Status {
		case primitive.PullFound:
			rlog.Debug(fmt.Sprintf("Topic: %s, QueueId: %d found messages.", request.mq.Topic, request.mq.QueueId), nil)
			prevRequestOffset := request.nextOffset
			request.nextOffset = result.NextBeginOffset

			rt := time.Now().Sub(beginTime) / time.Millisecond
			pc.stat.increasePullRT(pc.consumerGroup, request.mq.Topic, int64(rt))

			msgFounded := result.GetMessageExts()
			firstMsgOffset := int64(math.MaxInt64)
			if len(msgFounded) != 0 {
				firstMsgOffset = msgFounded[0].QueueOffset
				pc.stat.increasePullTPS(pc.consumerGroup, request.mq.Topic, len(msgFounded))
				pq.putMessage(msgFounded...)
			}
			if result.NextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset {
				rlog.Warning("[BUG] pull message result maybe data wrong", map[string]interface{}{
					"nextBeginOffset":   result.NextBeginOffset,
					"firstMsgOffset":    firstMsgOffset,
					"prevRequestOffset": prevRequestOffset,
				})
			}
		case primitive.PullNoNewMsg, primitive.PullNoMsgMatched:
			request.nextOffset = result.NextBeginOffset
			pc.correctTagsOffset(request)
		case primitive.PullOffsetIllegal:
			rlog.Warning("defaultPullConsumer the pull request offset illegal", map[string]interface{}{
				rlog.LogKeyPullRequest: request.String(),
				"result":               result.String(),
			})
			request.nextOffset = result.NextBeginOffset
			pq.WithDropped(true)
			time.Sleep(10 * time.Second)
			pc.storage.update(request.mq, request.nextOffset, false)
			pc.storage.persist([]*primitive.MessageQueue{request.mq})
			pc.processQueueTable.Delete(*request.mq)
			rlog.Warning(fmt.Sprintf("defaultPullConsumer fix the pull request offset: %s", request.String()), nil)
		default:
			rlog.Warning(fmt.Sprintf("defaultPullConsumer unknown pull status: %v", result.Status), nil)
			sleepTime = _PullDelayTimeWhenError
		}
	}
}

func (pc *defaultPullConsumer) correctTagsOffset(pr *PullRequest) {
	if pr.pq.cachedMsgCount.Load() <= 0 {
		pc.storage.update(pr.mq, pr.nextOffset, true)
	}
}

func (pc *defaultPullConsumer) consumeMessageConcurrently(pq *processQueue, mq *primitive.MessageQueue) {
	msgList := pq.getMessages()
	if msgList == nil {
		return
	}
	if pq.IsDroppd() {
		rlog.Info("defaultPullConsumer consumeMessageConcurrently the message queue not be able to consume, because it was dropped", map[string]interface{}{
			rlog.LogKeyMessageQueue:  mq.String(),
			rlog.LogKeyConsumerGroup: pc.consumerGroup,
		})
		return
	}
	cr := &ConsumeRequest{
		messageQueue: mq,
		processQueue: pq,
		msgList:      msgList,
	}

	select {
	case <-pq.closeChan:
		return
	case pc.consumeRequestCache <- cr:
	}
}

func (pc *defaultPullConsumer) GetConsumerStatus(topic string) *internal.ConsumerStatus {
	consumerStatus := internal.NewConsumerStatus()
	mqOffsetMap := pc.storage.getMQOffsetMap(topic)
	if mqOffsetMap != nil {
		consumerStatus.MQOffsetMap = mqOffsetMap
	}
	return consumerStatus
}

func (pc *defaultPullConsumer) validate() error {
	if err := internal.ValidateGroup(pc.consumerGroup); err != nil {
		return err
	}

	if pc.consumerGroup == internal.DefaultConsumerGroup {
		return fmt.Errorf("consumerGroup can't equal [%s], please specify another one", internal.DefaultConsumerGroup)
	}

	return nil
}
