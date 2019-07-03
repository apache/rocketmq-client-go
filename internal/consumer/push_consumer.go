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
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/apache/rocketmq-client-go/internal/kernel"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/apache/rocketmq-client-go/utils"
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

type PushConsumer interface {
	Start() error
	Shutdown()
	Subscribe(topic string, selector primitive.MessageSelector,
		f func(*primitive.ConsumeMessageContext, []*primitive.MessageExt) (primitive.ConsumeResult, error)) error
}

type pushConsumer struct {
	*defaultConsumer
	queueFlowControlTimes        int
	queueMaxSpanFlowControlTimes int
	consume                      func(*primitive.ConsumeMessageContext, []*primitive.MessageExt) (primitive.ConsumeResult, error)
	submitToConsume              func(*processQueue, *primitive.MessageQueue)
	subscribedTopic              map[string]string

	interceptor primitive.CInterceptor
}

func NewPushConsumer(consumerGroup string, opts primitive.ConsumerOptions) (PushConsumer, error) {
	if err := utils.VerifyIP(opts.NameServerAddr); err != nil {
		return nil, err
	}
	opts.InstanceName = "DEFAULT"
	opts.ClientIP = utils.LocalIP()
	if opts.NameServerAddr == "" {
		rlog.Fatal("opts.NameServerAddr can't be empty")
	}
	err := os.Setenv(kernel.EnvNameServerAddr, opts.NameServerAddr)
	if err != nil {
		rlog.Fatal("set env=EnvNameServerAddr error: %s ", err.Error())
	}
	dc := &defaultConsumer{
		consumerGroup:  consumerGroup,
		cType:          _PushConsume,
		state:          kernel.StateCreateJust,
		prCh:           make(chan PullRequest, 4),
		model:          opts.ConsumerModel,
		consumeOrderly: opts.ConsumeOrderly,
		fromWhere:      opts.FromWhere,
		option:         opts,
	}

	if opts.Strategy == nil {
		opts.Strategy = primitive.AllocateByAveragely
	}
	dc.allocate = opts.Strategy
	p := &pushConsumer{
		defaultConsumer: dc,
		subscribedTopic: make(map[string]string, 0),
	}
	dc.mqChanged = p.messageQueueChanged
	if p.consumeOrderly {
		p.submitToConsume = p.consumeMessageOrderly
	} else {
		p.submitToConsume = p.consumeMessageCurrently
	}

	ChainInterceptor(p)

	return p, nil
}

// ChainInterceptor chain list of interceptor as one interceptor
func ChainInterceptor(p *pushConsumer) {
	interceptors := p.option.Interceptors
	switch len(interceptors) {
	case 0:
		p.interceptor = nil
	case 1:
		p.interceptor = interceptors[0]
	default:
		p.interceptor = func(ctx *primitive.ConsumeMessageContext, msgs []*primitive.MessageExt, reply primitive.ConsumeResult, invoker primitive.CInvoker) error {
			return interceptors[0](ctx, msgs, reply, getChainedInterceptor(interceptors, 0, invoker))
		}
	}
}

// getChainedInterceptor recursively generate the chained invoker.
func getChainedInterceptor(interceptors []primitive.CInterceptor, cur int, finalInvoker primitive.CInvoker) primitive.CInvoker {
	if cur == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx *primitive.ConsumeMessageContext, msgs []*primitive.MessageExt, reply primitive.ConsumeResult) error {
		return interceptors[cur+1](ctx, msgs, reply, getChainedInterceptor(interceptors, cur+1, finalInvoker))
	}
}

func (pc *pushConsumer) Start() error {
	var err error
	pc.once.Do(func() {
		rlog.Infof("the consumerGroup=%s start beginning. messageModel=%v, unitMode=%v",
			pc.consumerGroup, pc.model, pc.unitMode)
		pc.state = kernel.StateStartFailed
		pc.validate()

		if pc.model == primitive.Clustering {
			// set retry topic
			retryTopic := kernel.GetRetryTopic(pc.consumerGroup)
			pc.subscriptionDataTable.Store(retryTopic, buildSubscriptionData(retryTopic,
				primitive.MessageSelector{primitive.TAG, _SubAll}))
		}

		pc.client = kernel.GetOrNewRocketMQClient(pc.option.ClientOption)
		if pc.model == primitive.Clustering {
			pc.option.ChangeInstanceNameToPID()
			pc.storage = NewRemoteOffsetStore(pc.consumerGroup, pc.client)
		} else {
			pc.storage = NewLocalFileOffsetStore(pc.consumerGroup, pc.client.ClientID())
		}
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

		err = pc.client.RegisterConsumer(pc.consumerGroup, pc)
		if err != nil {
			pc.state = kernel.StateCreateJust
			rlog.Errorf("the consumer group: [%s] has been created, specify another name.", pc.consumerGroup)
			err = errors.New("consumer group has been created")
			return
		}
		pc.client.UpdateTopicRouteInfo()
		pc.client.Start()
		pc.state = kernel.StateRunning
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

func (pc *pushConsumer) Shutdown() {}

func (pc *pushConsumer) Subscribe(topic string, selector primitive.MessageSelector,
	f func(*primitive.ConsumeMessageContext, []*primitive.MessageExt) (primitive.ConsumeResult, error)) error {
	if pc.state != kernel.StateCreateJust {
		return errors.New("subscribe topic only started before")
	}
	data := buildSubscriptionData(topic, selector)
	pc.subscriptionDataTable.Store(topic, data)
	pc.subscribedTopic[topic] = ""
	pc.consume = f
	return nil
}

func (pc *pushConsumer) Rebalance() {
	pc.defaultConsumer.doBalance()
}

func (pc *pushConsumer) PersistConsumerOffset() {
	pc.defaultConsumer.persistConsumerOffset()
}

func (pc *pushConsumer) UpdateTopicSubscribeInfo(topic string, mqs []*primitive.MessageQueue) {
	pc.defaultConsumer.updateTopicSubscribeInfo(topic, mqs)
}

func (pc *pushConsumer) IsSubscribeTopicNeedUpdate(topic string) bool {
	return pc.defaultConsumer.isSubscribeTopicNeedUpdate(topic)
}

func (pc *pushConsumer) SubscriptionDataList() []*kernel.SubscriptionData {
	return pc.defaultConsumer.SubscriptionDataList()
}

func (pc *pushConsumer) IsUnitMode() bool {
	return pc.unitMode
}

func (pc *pushConsumer) messageQueueChanged(topic string, mqAll, mqDivided []*primitive.MessageQueue) {
	// TODO
}

func (pc *pushConsumer) validate() {
	kernel.ValidateGroup(pc.consumerGroup)

	if pc.consumerGroup == kernel.DefaultConsumerGroup {
		// TODO FQA
		rlog.Fatalf("consumerGroup can't equal [%s], please specify another one.", kernel.DefaultConsumerGroup)
	}

	if len(pc.subscribedTopic) == 0 {
		rlog.Fatal("number of subscribed topics is 0.")
	}

	if pc.option.ConsumeConcurrentlyMaxSpan < 1 || pc.option.ConsumeConcurrentlyMaxSpan > 65535 {
		if pc.option.ConsumeConcurrentlyMaxSpan == 0 {
			pc.option.ConsumeConcurrentlyMaxSpan = 1000
		} else {
			rlog.Fatal("option.ConsumeConcurrentlyMaxSpan out of range [1, 65535]")
		}
	}

	if pc.option.PullThresholdForQueue < 1 || pc.option.PullThresholdForQueue > 65535 {
		if pc.option.PullThresholdForQueue == 0 {
			pc.option.PullThresholdForQueue = 1024
		} else {
			rlog.Fatal("option.PullThresholdForQueue out of range [1, 65535]")
		}
	}

	if pc.option.PullThresholdForTopic < 1 || pc.option.PullThresholdForTopic > 6553500 {
		if pc.option.PullThresholdForTopic == 0 {
			pc.option.PullThresholdForTopic = 102400
		} else {
			rlog.Fatal("option.PullThresholdForTopic out of range [1, 6553500]")
		}
	}

	if pc.option.PullThresholdSizeForQueue < 1 || pc.option.PullThresholdSizeForQueue > 1024 {
		if pc.option.PullThresholdSizeForQueue == 0 {
			pc.option.PullThresholdSizeForQueue = 512
		} else {
			rlog.Fatal("option.PullThresholdSizeForQueue out of range [1, 1024]")
		}
	}

	if pc.option.PullThresholdSizeForTopic < 1 || pc.option.PullThresholdSizeForTopic > 102400 {
		if pc.option.PullThresholdSizeForTopic == 0 {
			pc.option.PullThresholdSizeForTopic = 51200
		} else {
			rlog.Fatal("option.PullThresholdSizeForTopic out of range [1, 102400]")
		}
	}

	if pc.option.PullInterval < 0 || pc.option.PullInterval > 65535 {
		rlog.Fatal("option.PullInterval out of range [0, 65535]")
	}

	if pc.option.ConsumeMessageBatchMaxSize < 1 || pc.option.ConsumeMessageBatchMaxSize > 1024 {
		if pc.option.ConsumeMessageBatchMaxSize == 0 {
			pc.option.ConsumeMessageBatchMaxSize = 512
		} else {
			rlog.Fatal("option.ConsumeMessageBatchMaxSize out of range [1, 1024]")
		}
	}

	if pc.option.PullBatchSize < 1 || pc.option.PullBatchSize > 1024 {
		if pc.option.PullBatchSize == 0 {
			pc.option.PullBatchSize = 32
		} else {
			rlog.Fatal("option.PullBatchSize out of range [1, 1024]")
		}
	}
}

func (pc *pushConsumer) pullMessage(request *PullRequest) {
	rlog.Infof("start a nwe Pull Message task %s for [%s]", request.String(), pc.consumerGroup)
	var sleepTime time.Duration
	pq := request.pq
	go func() {
		for {
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
			rlog.Infof("pull MessageQueue: %d sleep %d ms", request.mq.QueueId, sleepTime/time.Millisecond)
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

		if pc.model == primitive.Clustering {
			commitOffsetValue = pc.storage.read(request.mq, _ReadFromMemory)
			if commitOffsetValue > 0 {
				commitOffsetEnable = true
			}
		}

		sd := v.(*kernel.SubscriptionData)
		classFilter := sd.ClassFilterMode
		if pc.option.PostSubscriptionWhenPull && classFilter {
			subExpression = sd.SubString
		}

		sysFlag := buildSysFlag(commitOffsetEnable, true, subExpression != "", classFilter)

		pullRequest := &kernel.PullMessageRequest{
			ConsumerGroup:  pc.consumerGroup,
			Topic:          request.mq.Topic,
			QueueId:        int32(request.mq.QueueId),
			QueueOffset:    request.nextOffset,
			MaxMsgNums:     pc.option.PullBatchSize,
			SysFlag:        sysFlag,
			CommitOffset:   commitOffsetValue,
			SubExpression:  _SubAll,
			ExpressionType: string(primitive.TAG), // TODO
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
			rlog.Debugf("Topic: %s, QueueId: %d found messages: %d", request.mq.Topic, request.mq.QueueId,
				len(result.GetMessageExts()))
			prevRequestOffset := request.nextOffset
			request.nextOffset = result.NextBeginOffset

			rt := time.Now().Sub(beginTime)
			increasePullRT(pc.consumerGroup, request.mq.Topic, rt)

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
			rlog.Debugf("Topic: %s, QueueId: %d no more msg, next offset: %d", request.mq.Topic, request.mq.QueueId, result.NextBeginOffset)
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

func (pc *pushConsumer) sendMessageBack(ctx *primitive.ConsumeMessageContext, msg *primitive.MessageExt) bool {
	return true
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
	//offsetTable := make(map[primitive.MessageQueue]int64, 0)
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
	queuesOfTopic := v.(map[int]*primitive.MessageQueue)
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
	if !pc.consumeOrderly || primitive.Clustering != pc.model {
		return true
	}
	// TODO orderly
	return true
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

			ctx := &primitive.ConsumeMessageContext{
				Properties: make(map[string]string),
			}
			// TODO hook
			beginTime := time.Now()
			groupTopic := kernel.RetryGroupTopicPrefix + pc.consumerGroup
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
			var result primitive.ConsumeResult
			err := pc.interceptor(ctx, subMsgs, result, func(ctx *primitive.ConsumeMessageContext, msgs []*primitive.MessageExt, reply primitive.ConsumeResult) error {
				reply, err := pc.consume(ctx, subMsgs)
				return err
			})

			consumeRT := time.Now().Sub(beginTime)
			if err != nil {
				ctx.Properties["ConsumeContextType"] = "EXCEPTION"
			} else if consumeRT >= pc.option.ConsumeTimeout {
				ctx.Properties["ConsumeContextType"] = "TIMEOUT"
			} else if result == primitive.ConsumeSuccess {
				ctx.Properties["ConsumeContextType"] = "SUCCESS"
			} else {
				ctx.Properties["ConsumeContextType"] = "RECONSUME_LATER"
			}

			// TODO hook
			increaseConsumeRT(pc.consumerGroup, mq.Topic, consumeRT)

			if !pq.dropped {
				msgBackFailed := make([]*primitive.MessageExt, 0)
				if result == primitive.ConsumeSuccess {
					increaseConsumeOKTPS(pc.consumerGroup, mq.Topic, len(subMsgs))
				} else {
					increaseConsumeFailedTPS(pc.consumerGroup, mq.Topic, len(subMsgs))
					if pc.model == primitive.BroadCasting {
						for i := 0; i < len(msgs); i++ {
							rlog.Warnf("BROADCASTING, the message=%s consume failed, drop it, {}", subMsgs[i])
						}
					} else {
						for i := 0; i < len(msgs); i++ {
							msg := msgs[i]
							if !pc.sendMessageBack(ctx, msg) {
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
}
