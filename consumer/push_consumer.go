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
	"strconv"
	"time"

	"github.com/apache/rocketmq-client-go/internal"
	"github.com/apache/rocketmq-client-go/internal/remote"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/pkg/errors"
)

// In most scenarios, this is the mostly recommended usage to consume messages.
//
// Technically speaking, this push client is virtually a wrapper of the underlying pull service. Specifically, on
// arrival of messages pulled from brokers, it roughly invokes the registered callback handler to feed the messages.
//
// See quick start/Consumer in the example module for a typical usage.
//
// <strong>Thread Safety:</strong> After initialization, the instance can be regarded as thread-safe.

const (
	Mb = 1024 * 1024
)

type pushConsumer struct {
	*defaultConsumer
	queueFlowControlTimes        int
	queueMaxSpanFlowControlTimes int
	consume                      func(context.Context, ...*primitive.MessageExt) (ConsumeResult, error)
	submitToConsume              func(*processQueue, *primitive.MessageQueue)
	subscribedTopic              map[string]string
	interceptor                  primitive.Interceptor
	queueLock                    *QueueLock
}

func NewPushConsumer(opts ...Option) (*pushConsumer, error) {
	defaultOpts := defaultPushConsumerOptions()
	for _, apply := range opts {
		apply(&defaultOpts)
	}
	srvs, err := internal.NewNamesrv(defaultOpts.NameServerAddrs...)
	if err != nil {
		return nil, errors.Wrap(err, "new Namesrv failed.")
	}
	internal.RegisterNamsrv(srvs)

	dc := &defaultConsumer{
		consumerGroup:  defaultOpts.GroupName,
		cType:          _PushConsume,
		state:          internal.StateCreateJust,
		prCh:           make(chan PullRequest, 4),
		model:          defaultOpts.ConsumerModel,
		consumeOrderly: defaultOpts.ConsumeOrderly,
		fromWhere:      defaultOpts.FromWhere,
		allocate:       defaultOpts.Strategy,
		option:         defaultOpts,
	}

	p := &pushConsumer{
		defaultConsumer: dc,
		subscribedTopic: make(map[string]string, 0),
		queueLock:       newQueueLock(),
	}
	dc.mqChanged = p.messageQueueChanged
	if p.consumeOrderly {
		p.submitToConsume = p.consumeMessageOrderly
	} else {
		p.submitToConsume = p.consumeMessageCurrently
	}

	chainInterceptor(p)

	return p, nil
}

// chainInterceptor chain list of interceptor as one interceptor
func chainInterceptor(p *pushConsumer) {
	interceptors := p.option.Interceptors
	switch len(interceptors) {
	case 0:
		p.interceptor = nil
	case 1:
		p.interceptor = interceptors[0]
	default:
		p.interceptor = func(ctx context.Context, req, reply interface{}, invoker primitive.Invoker) error {
			return interceptors[0](ctx, req, reply, getChainedInterceptor(interceptors, 0, invoker))
		}
	}
}

// getChainedInterceptor recursively generate the chained invoker.
func getChainedInterceptor(interceptors []primitive.Interceptor, cur int, finalInvoker primitive.Invoker) primitive.Invoker {
	if cur == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx context.Context, req, reply interface{}) error {
		return interceptors[cur+1](ctx, req, reply, getChainedInterceptor(interceptors, cur+1, finalInvoker))
	}
}

// TODO: add shutdown on pushConsumr.
func (pc *pushConsumer) Start() error {
	var err error
	pc.once.Do(func() {
		rlog.Infof("the consumerGroup=%s start beginning. messageModel=%v, unitMode=%v",
			pc.consumerGroup, pc.model, pc.unitMode)
		pc.state = internal.StateStartFailed
		pc.validate()

		err = pc.defaultConsumer.start()
		if err != nil {
			return
		}

		err := pc.client.RegisterConsumer(pc.consumerGroup, pc)
		if err != nil {
			pc.state = internal.StateStartFailed
			rlog.Errorf("the consumer group: [%s] has been created, specify another name.", pc.consumerGroup)
			err = ErrCreated
		}

		go func() {
			// initial lock.
			time.Sleep(1000 * time.Millisecond)
			pc.lockAll()

			t := time.NewTicker(pc.option.RebalanceLockInterval)
			for range t.C {
				pc.lockAll()
			}
		}()
		go func() {
			// todo start clean msg expired
			// TODO quit
			for {
				pr := <-pc.prCh
				go func() {
					pc.pullMessage(&pr)
				}()
			}
		}()
	})

	pc.client.UpdateTopicRouteInfo()
	for k := range pc.subscribedTopic {
		_, exist := pc.topicSubscribeInfoTable.Load(k)
		if !exist {
			pc.client.Shutdown()
			return fmt.Errorf("the topic=%s route info not found, it may not exist", k)
		}
	}
	pc.client.RebalanceImmediately()
	pc.client.CheckClientInBroker()
	pc.client.SendHeartbeatToAllBrokerWithLock()

	return err
}

func (pc *pushConsumer) Shutdown() error {
	return nil
}

func (pc *pushConsumer) Subscribe(topic string, selector MessageSelector,
	f func(context.Context, ...*primitive.MessageExt) (ConsumeResult, error)) error {
	if pc.state != internal.StateCreateJust {
		return errors.New("subscribe topic only started before")
	}
	data := buildSubscriptionData(topic, selector)
	pc.subscriptionDataTable.Store(topic, data)
	pc.subscribedTopic[topic] = ""

	if pc.option.ConsumerModel == Clustering {
		// add retry topic for clustering mode
		retryTopic := internal.GetRetryTopic(pc.consumerGroup)
		data = buildSubscriptionData(retryTopic, MessageSelector{Expression: _SubAll})
		pc.subscriptionDataTable.Store(retryTopic, data)
		pc.subscribedTopic[retryTopic] = ""
	}

	pc.consume = f
	return nil
}

func (pc *pushConsumer) Unsubscribe(string) error {
	return nil
}

func (pc *pushConsumer) Rebalance() {
	pc.defaultConsumer.doBalance()
}

func (pc *pushConsumer) PersistConsumerOffset() error {
	return pc.defaultConsumer.persistConsumerOffset()
}

func (pc *pushConsumer) UpdateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue) {
	pc.defaultConsumer.updateTopicSubscribeInfo(topic, mqs)
}

func (pc *pushConsumer) IsSubscribeTopicNeedUpdate(topic string) bool {
	return pc.defaultConsumer.isSubscribeTopicNeedUpdate(topic)
}

func (pc *pushConsumer) SubscriptionDataList() []*internal.SubscriptionData {
	return pc.defaultConsumer.SubscriptionDataList()
}

func (pc *pushConsumer) IsUnitMode() bool {
	return pc.unitMode
}

func (pc *pushConsumer) messageQueueChanged(topic string, mqAll, mqDivided []*primitive.MessageQueue) {
	// TODO
}

func (pc *pushConsumer) validate() {
	internal.ValidateGroup(pc.consumerGroup)

	if pc.consumerGroup == internal.DefaultConsumerGroup {
		// TODO FQA
		rlog.Errorf("consumerGroup can't equal [%s], please specify another one.", internal.DefaultConsumerGroup)
	}

	if len(pc.subscribedTopic) == 0 {
		rlog.Error("number of subscribed topics is 0.")
	}

	if pc.option.ConsumeConcurrentlyMaxSpan < 1 || pc.option.ConsumeConcurrentlyMaxSpan > 65535 {
		if pc.option.ConsumeConcurrentlyMaxSpan == 0 {
			pc.option.ConsumeConcurrentlyMaxSpan = 1000
		} else {
			rlog.Error("option.ConsumeConcurrentlyMaxSpan out of range [1, 65535]")
		}
	}

	if pc.option.PullThresholdForQueue < 1 || pc.option.PullThresholdForQueue > 65535 {
		if pc.option.PullThresholdForQueue == 0 {
			pc.option.PullThresholdForQueue = 1024
		} else {
			rlog.Error("option.PullThresholdForQueue out of range [1, 65535]")
		}
	}

	if pc.option.PullThresholdForTopic < 1 || pc.option.PullThresholdForTopic > 6553500 {
		if pc.option.PullThresholdForTopic == 0 {
			pc.option.PullThresholdForTopic = 102400
		} else {
			rlog.Error("option.PullThresholdForTopic out of range [1, 6553500]")
		}
	}

	if pc.option.PullThresholdSizeForQueue < 1 || pc.option.PullThresholdSizeForQueue > 1024 {
		if pc.option.PullThresholdSizeForQueue == 0 {
			pc.option.PullThresholdSizeForQueue = 512
		} else {
			rlog.Error("option.PullThresholdSizeForQueue out of range [1, 1024]")
		}
	}

	if pc.option.PullThresholdSizeForTopic < 1 || pc.option.PullThresholdSizeForTopic > 102400 {
		if pc.option.PullThresholdSizeForTopic == 0 {
			pc.option.PullThresholdSizeForTopic = 51200
		} else {
			rlog.Error("option.PullThresholdSizeForTopic out of range [1, 102400]")
		}
	}

	if pc.option.PullInterval < 0 || pc.option.PullInterval > 65535 {
		rlog.Error("option.PullInterval out of range [0, 65535]")
	}

	if pc.option.ConsumeMessageBatchMaxSize < 1 || pc.option.ConsumeMessageBatchMaxSize > 1024 {
		if pc.option.ConsumeMessageBatchMaxSize == 0 {
			pc.option.ConsumeMessageBatchMaxSize = 512
		} else {
			rlog.Error("option.ConsumeMessageBatchMaxSize out of range [1, 1024]")
		}
	}

	if pc.option.PullBatchSize < 1 || pc.option.PullBatchSize > 1024 {
		if pc.option.PullBatchSize == 0 {
			pc.option.PullBatchSize = 32
		} else {
			rlog.Error("option.PullBatchSize out of range [1, 1024]")
		}
	}
}

func (pc *pushConsumer) pullMessage(request *PullRequest) {
	rlog.Infof("start a nwe Pull Message task %s for [%s]", request.String(), pc.consumerGroup)
	var sleepTime time.Duration
	pq := request.pq
	go func() {
		for {
			// TODO: add exit logic
			pc.submitToConsume(request.pq, request.mq)
		}
	}()

	for {
	NEXT:
		if pq.dropped {
			rlog.Infof("the request: [%s] was dropped, so stop task", request.String())
			return
		}
		if sleepTime > 0 {
			rlog.Infof("pull MessageQueue: %d sleep %d ms for mq: %v", request.mq.QueueId, sleepTime/time.Millisecond, request.mq)
			time.Sleep(sleepTime)
		}
		// reset time
		sleepTime = pc.option.PullInterval
		pq.lastPullTime = time.Now()
		err := pc.makeSureStateOK()
		if err != nil {
			rlog.Warnf("consumer state error: %s", err.Error())
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		if pc.pause {
			rlog.Infof("consumer [%s] of [%s] was paused, execute pull request [%s] later",
				pc.option.InstanceName, pc.consumerGroup, request.String())
			sleepTime = _PullDelayTimeWhenSuspend
			goto NEXT
		}

		cachedMessageSizeInMiB := int(pq.cachedMsgSize / Mb)
		if pq.cachedMsgCount > pc.option.PullThresholdForQueue {
			if pc.queueFlowControlTimes%1000 == 0 {
				rlog.Warnf("the cached message count exceeds the threshold %d, so do flow control, "+
					"minOffset=%d, maxOffset=%d, count=%d, size=%d MiB, pullRequest=%s, flowControlTimes=%d",
					pc.option.PullThresholdForQueue, 0, pq.Min(), pq.Max(),
					pq.msgCache, cachedMessageSizeInMiB, request.String(), pc.queueFlowControlTimes)
			}
			pc.queueFlowControlTimes++
			sleepTime = _PullDelayTimeWhenFlowControl
			goto NEXT
		}

		if cachedMessageSizeInMiB > pc.option.PullThresholdSizeForQueue {
			if pc.queueFlowControlTimes%1000 == 0 {
				rlog.Warnf("the cached message size exceeds the threshold %d MiB, so do flow control, "+
					"minOffset=%d, maxOffset=%d, count=%d, size=%d MiB, pullRequest=%s, flowControlTimes=%d",
					pc.option.PullThresholdSizeForQueue, pq.Min(), pq.Max(),
					pq.msgCache, cachedMessageSizeInMiB, request.String(), pc.queueFlowControlTimes)
			}
			pc.queueFlowControlTimes++
			sleepTime = _PullDelayTimeWhenFlowControl
			goto NEXT
		}

		if !pc.consumeOrderly {
			if pq.getMaxSpan() > pc.option.ConsumeConcurrentlyMaxSpan {

				if pc.queueMaxSpanFlowControlTimes%1000 == 0 {
					rlog.Warnf("the queue's messages, span too long, limit=%d, so do flow control, minOffset=%d, "+
						"maxOffset=%d, maxSpan=%d, pullRequest=%s, flowControlTimes=%d", pc.option.ConsumeConcurrentlyMaxSpan,
						pq.Min(), pq.Max(), pq.getMaxSpan(), request.String(), pc.queueMaxSpanFlowControlTimes)
				}
				sleepTime = _PullDelayTimeWhenFlowControl
				goto NEXT
			}
		} else {
			if pq.locked {
				if !request.lockedFirst {
					offset := pc.computePullFromWhere(request.mq)
					brokerBusy := offset < request.nextOffset
					rlog.Infof("the first time to pull message, so fix offset from broker. "+
						"pullRequest: [%s] NewOffset: %d brokerBusy: %v",
						request.String(), offset, brokerBusy)
					if brokerBusy {
						rlog.Infof("[NOTIFY_ME]the first time to pull message, but pull request offset"+
							" larger than broker consume offset. pullRequest: [%s] NewOffset: %d",
							request.String(), offset)
					}
					request.lockedFirst = true
					request.nextOffset = offset
				}
			} else {
				rlog.Infof("pull message later because not locked in broker, [%s]", request.String())
				sleepTime = _PullDelayTimeWhenError
				goto NEXT
			}
		}

		v, exist := pc.subscriptionDataTable.Load(request.mq.Topic)
		if !exist {
			rlog.Warnf("find the consumer's subscription failed, %s", request.String())
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}
		beginTime := time.Now()
		var (
			commitOffsetEnable bool
			commitOffsetValue  int64
			subExpression      string
		)

		if pc.model == Clustering {
			commitOffsetValue = pc.storage.read(request.mq, _ReadFromMemory)
			if commitOffsetValue > 0 {
				commitOffsetEnable = true
			}
		}

		sd := v.(*internal.SubscriptionData)
		classFilter := sd.ClassFilterMode
		if pc.option.PostSubscriptionWhenPull && classFilter {
			subExpression = sd.SubString
		}

		sysFlag := buildSysFlag(commitOffsetEnable, true, subExpression != "", classFilter)

		pullRequest := &internal.PullMessageRequest{
			ConsumerGroup:  pc.consumerGroup,
			Topic:          request.mq.Topic,
			QueueId:        int32(request.mq.QueueId),
			QueueOffset:    request.nextOffset,
			MaxMsgNums:     pc.option.PullBatchSize,
			SysFlag:        sysFlag,
			CommitOffset:   commitOffsetValue,
			SubExpression:  _SubAll,
			ExpressionType: string(TAG), // TODO
		}
		//
		//if data.ExpType == string(TAG) {
		//	pullRequest.SubVersion = 0
		//} else {
		//	pullRequest.SubVersion = data.SubVersion
		//}

		brokerResult := tryFindBroker(request.mq)
		if brokerResult == nil {
			rlog.Warnf("no broker found for %s", request.mq.String())
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		result, err := pc.client.PullMessage(context.Background(), brokerResult.BrokerAddr, pullRequest)
		if err != nil {
			rlog.Warnf("pull message from %s error: %s", brokerResult.BrokerAddr, err.Error())
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		if result.Status == primitive.PullBrokerTimeout {
			rlog.Warnf("pull broker: %s timeout", brokerResult.BrokerAddr)
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		switch result.Status {
		case primitive.PullFound:
			rlog.Infof("Topic: %s, QueueId: %d found messages.", request.mq.Topic, request.mq.QueueId)
			prevRequestOffset := request.nextOffset
			request.nextOffset = result.NextBeginOffset

			rt := time.Now().Sub(beginTime) / time.Millisecond
			increasePullRT(pc.consumerGroup, request.mq.Topic, int64(rt))

			result.SetMessageExts(primitive.DecodeMessage(result.GetBody()))

			msgFounded := result.GetMessageExts()
			firstMsgOffset := int64(math.MaxInt64)
			if msgFounded != nil && len(msgFounded) != 0 {
				firstMsgOffset = msgFounded[0].QueueOffset
				increasePullTPS(pc.consumerGroup, request.mq.Topic, len(msgFounded))
				pq.putMessage(msgFounded...)
			}
			if result.NextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset {
				rlog.Warnf("[BUG] pull message result maybe data wrong, [nextBeginOffset=%d, "+
					"firstMsgOffset=%d, prevRequestOffset=%d]", result.NextBeginOffset, firstMsgOffset, prevRequestOffset)
			}
		case primitive.PullNoNewMsg:
			rlog.Infof("Topic: %s, QueueId: %d no more msg, current offset: %d, next offset: %d", request.mq.Topic, request.mq.QueueId, pullRequest.QueueOffset, result.NextBeginOffset)
		case primitive.PullNoMsgMatched:
			request.nextOffset = result.NextBeginOffset
			pc.correctTagsOffset(request)
		case primitive.PullOffsetIllegal:
			rlog.Warnf("the pull request offset illegal, {} {}", request.String(), result.String())
			request.nextOffset = result.NextBeginOffset
			pq.dropped = true
			go func() {
				time.Sleep(10 * time.Second)
				pc.storage.update(request.mq, request.nextOffset, false)
				pc.storage.persist([]*primitive.MessageQueue{request.mq})
				pc.storage.remove(request.mq)
				rlog.Warnf("fix the pull request offset: %s", request.String())
			}()
		default:
			rlog.Warnf("unknown pull status: %v", result.Status)
			sleepTime = _PullDelayTimeWhenError
		}
	}
}

func (pc *pushConsumer) correctTagsOffset(pr *PullRequest) {
	// TODO
}

func (pc *pushConsumer) sendMessageBack(brokerName string, msg *primitive.MessageExt, delayLevel int) bool {
	var brokerAddr string
	if len(brokerName) != 0 {
		brokerAddr = internal.FindBrokerAddrByName(brokerName)
	} else {
		brokerAddr = msg.StoreHost
	}
	_, err := pc.client.InvokeSync(brokerAddr, pc.buildSendBackRequest(msg, delayLevel), 3*time.Second)
	if err != nil {
		return false
	}
	return true
}

func (pc *pushConsumer) buildSendBackRequest(msg *primitive.MessageExt, delayLevel int) *remote.RemotingCommand {
	req := &internal.ConsumerSendMsgBackRequest{
		Group:             pc.consumerGroup,
		OriginTopic:       msg.Topic,
		Offset:            msg.CommitLogOffset,
		DelayLevel:        delayLevel,
		OriginMsgId:       msg.MsgId,
		MaxReconsumeTimes: pc.getMaxReconsumeTimes(),
	}

	return remote.NewRemotingCommand(internal.ReqConsumerSendMsgBack, req, msg.Body)
}

func (pc *pushConsumer) suspend() {
	pc.pause = true
	rlog.Infof("suspend consumer: %s", pc.consumerGroup)
}

func (pc *pushConsumer) resume() {
	pc.pause = false
	pc.doBalance()
	rlog.Infof("resume consumer: %s", pc.consumerGroup)
}

func (pc *pushConsumer) resetOffset(topic string, table map[primitive.MessageQueue]int64) {
	//topic := cmd.ExtFields["topic"]
	//group := cmd.ExtFields["group"]
	//if topic == "" || group == "" {
	//	rlog.Warnf("received reset offset command from: %s, but missing params.", from)
	//	return
	//}
	//t, err := strconv.ParseInt(cmd.ExtFields["timestamp"], 10, 64)
	//if err != nil {
	//	rlog.Warnf("received reset offset command from: %s, but parse time error: %s", err.Error())
	//	return
	//}
	//rlog.Infof("invoke reset offset operation from broker. brokerAddr=%s, topic=%s, group=%s, timestamp=%v",
	//	from, topic, group, t)
	//
	//offsetTable := make(map[MessageQueue]int64, 0)
	//err = json.Unmarshal(cmd.Body, &offsetTable)
	//if err != nil {
	//	rlog.Warnf("received reset offset command from: %s, but parse offset table: %s", err.Error())
	//	return
	//}
	//v, exist := c.consumerMap.Load(group)
	//if !exist {
	//	rlog.Infof("[reset-offset] consumer dose not exist. group=%s", group)
	//	return
	//}

	set := make(map[int]*primitive.MessageQueue, 0)
	for k := range table {
		set[k.HashCode()] = &k
	}
	pc.processQueueTable.Range(func(key, value interface{}) bool {
		mqHash := value.(int)
		pq := value.(*processQueue)
		if set[mqHash] != nil {
			pq.dropped = true
			pq.clear()
		}
		return true
	})
	time.Sleep(10 * time.Second)
	v, exist := pc.topicSubscribeInfoTable.Load(topic)
	if !exist {
		return
	}
	queuesOfTopic := v.(map[int]primitive.MessageQueue)
	for k := range queuesOfTopic {
		q := set[k]
		if q != nil {
			pc.storage.update(q, table[*q], false)
			v, exist := pc.processQueueTable.Load(k)
			if !exist {
				continue
			}
			pq := v.(*processQueue)
			pc.removeUnnecessaryMessageQueue(q, pq)
			delete(queuesOfTopic, k)
		}
	}
}

func (pc *pushConsumer) removeUnnecessaryMessageQueue(mq *primitive.MessageQueue, pq *processQueue) bool {
	pc.defaultConsumer.removeUnnecessaryMessageQueue(mq, pq)
	if !pc.consumeOrderly || Clustering != pc.model {
		return true
	}
	// TODO orderly
	return true
}

func (pc *pushConsumer) consumeInner(ctx context.Context, subMsgs []*primitive.MessageExt) (ConsumeResult, error) {

	if pc.interceptor == nil {
		return pc.consume(ctx, subMsgs...)
	} else {
		var container ConsumeResultHolder
		err := pc.interceptor(ctx, subMsgs, &container, func(ctx context.Context, req, reply interface{}) error {
			msgs := req.([]*primitive.MessageExt)
			r, e := pc.consume(ctx, msgs...)

			realReply := reply.(*ConsumeResultHolder)
			realReply.ConsumeResult = r
			return e
		})
		return container.ConsumeResult, err
	}
}

// resetRetryAndNamespace modify retry message.
func (pc *pushConsumer) resetRetryAndNamespace(subMsgs []*primitive.MessageExt) {
	groupTopic := internal.RetryGroupTopicPrefix + pc.consumerGroup
	beginTime := time.Now()
	for idx := range subMsgs {
		msg := subMsgs[idx]
		if msg.Properties != nil {
			retryTopic := msg.Properties[primitive.PropertyRetryTopic]
			if retryTopic == "" && groupTopic == msg.Topic {
				msg.Topic = retryTopic
			}
			subMsgs[idx].Properties[primitive.PropertyConsumeStartTime] = strconv.FormatInt(
				beginTime.UnixNano()/int64(time.Millisecond), 10)
		}
	}
}

func (pc *pushConsumer) consumeMessageCurrently(pq *processQueue, mq *primitive.MessageQueue) {
	msgs := pq.getMessages()
	if msgs == nil {
		return
	}
	for count := 0; count < len(msgs); count++ {
		var subMsgs []*primitive.MessageExt
		if count+pc.option.ConsumeMessageBatchMaxSize > len(msgs) {
			subMsgs = msgs[count:]
			count = len(msgs)
		} else {
			next := count + pc.option.ConsumeMessageBatchMaxSize
			subMsgs = msgs[count:next]
			count = next
		}
		go func() {
		RETRY:
			if pq.dropped {
				rlog.Infof("the message queue not be able to consume, because it was dropped. group=%s, mq=%s",
					pc.consumerGroup, mq.String())
				return
			}

			// TODO hook
			beginTime := time.Now()
			pc.resetRetryAndNamespace(subMsgs)
			var result ConsumeResult

			var err error
			msgCtx := &primitive.ConsumeMessageContext{
				Properties: make(map[string]string),
			}
			ctx := context.Background()
			ctx = primitive.WithConsumerCtx(ctx, msgCtx)
			ctx = primitive.WithMethod(ctx, primitive.ConsumerPush)
			concurrentCtx := primitive.NewConsumeConcurrentlyContext()
			concurrentCtx.MQ = *mq
			ctx = primitive.WithConcurrentlyCtx(ctx, concurrentCtx)

			result, err = pc.consumeInner(ctx, subMsgs)

			consumeRT := time.Now().Sub(beginTime)
			if err != nil {
				msgCtx.Properties["ConsumeContextType"] = "EXCEPTION"
			} else if consumeRT >= pc.option.ConsumeTimeout {
				msgCtx.Properties["ConsumeContextType"] = "TIMEOUT"
			} else if result == ConsumeSuccess {
				msgCtx.Properties["ConsumeContextType"] = "SUCCESS"
			} else {
				msgCtx.Properties["ConsumeContextType"] = "RECONSUME_LATER"
			}

			// TODO hook
			increaseConsumeRT(pc.consumerGroup, mq.Topic, int64(consumeRT/time.Millisecond))

			if !pq.dropped {
				msgBackFailed := make([]*primitive.MessageExt, 0)
				if result == ConsumeSuccess {
					increaseConsumeOKTPS(pc.consumerGroup, mq.Topic, len(subMsgs))
				} else {
					increaseConsumeFailedTPS(pc.consumerGroup, mq.Topic, len(subMsgs))
					if pc.model == BroadCasting {
						for i := 0; i < len(msgs); i++ {
							rlog.Warnf("BROADCASTING, the message=%s consume failed, drop it, {}", subMsgs[i])
						}
					} else {
						for i := 0; i < len(msgs); i++ {
							msg := msgs[i]
							if !pc.sendMessageBack(mq.BrokerName, msg, concurrentCtx.DelayLevelWhenNextConsume) {
								msg.ReconsumeTimes += 1
								msgBackFailed = append(msgBackFailed, msg)
							}
						}
					}
				}

				offset := pq.removeMessage(subMsgs...)

				if offset >= 0 && !pq.dropped {
					pc.storage.update(mq, int64(offset), true)
				}
				if len(msgBackFailed) > 0 {
					subMsgs = msgBackFailed
					time.Sleep(5 * time.Second)
					goto RETRY
				}
			} else {
				rlog.Warnf("processQueue is dropped without process consume result. messageQueue=%s, msgs=%+v",
					mq, msgs)
			}
		}()
	}
}

func (pc *pushConsumer) consumeMessageOrderly(pq *processQueue, mq *primitive.MessageQueue) {
	if pq.dropped {
		rlog.Warn("the message queue not be able to consume, because it's dropped.")
		return
	}

	lock := pc.queueLock.fetchLock(*mq)
	lock.Lock()
	defer lock.Unlock()
	if pc.model == BroadCasting || (pq.locked && !pq.isLockExpired()) {
		beginTime := time.Now()

		continueConsume := true
		for continueConsume {
			if pq.dropped {
				rlog.Warn("the message queue not be able to consume, because it's dropped. %v", mq)
				break
			}
			if pc.model == Clustering {
				if !pq.locked {
					rlog.Warn("the message queue not locked, so consume later: %v", mq)
					pc.tryLocakLaterAndReconsume(mq, 10)
					return
				}
				if pq.isLockExpired() {
					rlog.Warn("the message queue lock expired, so consume later: %v", mq)
					pc.tryLocakLaterAndReconsume(mq, 10)
					return
				}
			}
			interval := time.Now().Sub(beginTime)
			if interval > pc.option.MaxTimeConsumeContinuously {
				time.Sleep(10 * time.Millisecond)
				return
			}
			batchSize := pc.option.ConsumeMessageBatchMaxSize
			msgs := pq.takeMessages(batchSize)

			pc.resetRetryAndNamespace(msgs)

			if len(msgs) == 0 {
				continueConsume = false
				break
			}

			// TODO: add message consumer hook
			beginTime = time.Now()

			ctx := context.Background()
			msgCtx := &primitive.ConsumeMessageContext{
				Properties: make(map[string]string),
			}
			ctx = primitive.WithConsumerCtx(ctx, msgCtx)
			ctx = primitive.WithMethod(ctx, primitive.ConsumerPush)

			orderlyCtx := primitive.NewConsumeOrderlyContext()
			orderlyCtx.MQ = *mq
			ctx = primitive.WithOrderlyCtx(ctx, orderlyCtx)

			pq.lockConsume.Lock()
			result, _ := pc.consumeInner(ctx, msgs)
			pq.lockConsume.Unlock()

			if result == Rollback || result == SuspendCurrentQueueAMoment {
				rlog.Warn("consumeMessage Orderly return not OK, Group: %v Msgs: %v MQ: %v",
					pc.consumerGroup, msgs, mq)
			}

			// jsut put consumeResult in consumerMessageCtx
			//interval = time.Now().Sub(beginTime)
			//consumeReult := SuccessReturn
			//if interval > pc.option.ConsumeTimeout {
			//	consumeReult = TimeoutReturn
			//} else if SuspendCurrentQueueAMoment == result {
			//	consumeReult = FailedReturn
			//} else if ConsumeSuccess == result {
			//	consumeReult = SuccessReturn
			//}

			// process result
			commitOffset := int64(-1)
			if pc.option.AutoCommit {
				switch result {
				case Commit, Rollback:
					rlog.Warn("the message queue consume result is illegal, we think you want to ack these message: %v", mq)
				case ConsumeSuccess:
					commitOffset = pq.commit()
				case SuspendCurrentQueueAMoment:
					if (pc.checkReconsumeTimes(msgs)) {
						pq.putMessage(msgs...)
						time.Sleep(time.Duration(orderlyCtx.SuspendCurrentQueueTimeMillis) * time.Millisecond)
						continueConsume = false;
					} else {
						commitOffset = pq.commit()
					}
				default:
				}
			} else {
				switch result {
				case ConsumeSuccess:
				case Commit:
					commitOffset = pq.commit();
				case Rollback:
					// pq.rollback
					time.Sleep(time.Duration(orderlyCtx.SuspendCurrentQueueTimeMillis) * time.Millisecond)
					continueConsume = false
				case SuspendCurrentQueueAMoment:
					if (pc.checkReconsumeTimes(msgs)) {
						time.Sleep(time.Duration(orderlyCtx.SuspendCurrentQueueTimeMillis) * time.Millisecond)
						continueConsume = false;
					}
				default:
				}
			}
			if commitOffset > 0 && !pq.dropped {
				pc.updateOffset(mq, commitOffset)
			}
		}
	} else {
		if pq.dropped {
			rlog.Warn("the message queue not be able to consume, because it's dropped. %v", mq)
		}
		pc.tryLocakLaterAndReconsume(mq, 100)
	}
}

func (pc *pushConsumer) checkReconsumeTimes(msgs []*primitive.MessageExt) bool {
	suspend := false
	if len(msgs) != 0 {
		maxReconsumeTimes := pc.getOrderlyMaxReconsumeTimes()
		for _, msg := range msgs {
			if msg.ReconsumeTimes > maxReconsumeTimes {
				rlog.Warn("msg will be send to retry topic due to ReconsumeTimes > %d, \n", maxReconsumeTimes)
				msg.Properties["RECONSUME_TIME"] = strconv.Itoa(int(msg.ReconsumeTimes))
				if !pc.sendMessageBack("", msg, -1) {
					suspend = true
					msg.ReconsumeTimes += 1
				}
			} else {
				suspend = true
				msg.ReconsumeTimes += 1
			}
		}
	}
	return suspend
}

func (pc *pushConsumer) getOrderlyMaxReconsumeTimes() int32 {
	if pc.option.MaxReconsumeTimes == -1 {
		return math.MaxInt32
	} else {
		return pc.option.MaxReconsumeTimes
	}
}

func (pc *pushConsumer) getMaxReconsumeTimes() int32 {
	if pc.option.MaxReconsumeTimes == -1 {
		return 16
	} else {
		return pc.option.MaxReconsumeTimes
	}
}

func (pc *pushConsumer) tryLocakLaterAndReconsume(mq *primitive.MessageQueue, delay int64) {
	time.Sleep(time.Duration(delay) * time.Millisecond)
	if pc.lock(mq) == true {
		pc.submitConsumeRequestLater(10)
	} else {
		pc.submitConsumeRequestLater(3000)
	}
}

func (pc *pushConsumer) submitConsumeRequestLater(suspendTimeMillis int64) {
	if suspendTimeMillis == -1 {
		suspendTimeMillis = int64(pc.option.SuspendCurrentQueueTimeMillis / time.Millisecond)
	}
	if suspendTimeMillis < 10 {
		suspendTimeMillis = 10
	} else if suspendTimeMillis > 30000 {
		suspendTimeMillis = 30000
	}
	time.Sleep(time.Duration(suspendTimeMillis) * time.Millisecond)
}
