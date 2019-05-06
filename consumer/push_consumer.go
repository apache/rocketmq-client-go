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
	"github.com/apache/rocketmq-client-go/kernel"
	"github.com/apache/rocketmq-client-go/rlog"
	"math"
	"strconv"
	"time"
)

// In most scenarios, this is the mostly recommended usage to consume messages.
//
// Technically speaking, this push client is virtually a wrapper of the underlying pull service. Specifically, on
// arrival of messages pulled from brokers, it roughly invokes the registered callback handler to feed the messages.
//
// See quick start/Consumer in the example module for a typical usage.
//
// <strong>Thread Safety:</strong> After initialization, the instance can be regarded as thread-safe.
type ConsumeResult int

const (
	Mb                           = 1024 * 1024
	ConsumeSuccess ConsumeResult = iota
	ConsumeRetryLater
)

type PushConsumer interface {
	Start() error
	Shutdown()
	Subscribe(topic string, selector MessageSelector,
		f func(*ConsumeMessageContext, []*kernel.MessageExt) (ConsumeResult, error)) error
}

type pushConsumer struct {
	*defaultConsumer
	queueFlowControlTimes        int
	queueMaxSpanFlowControlTimes int
	consume                      func(*ConsumeMessageContext, []*kernel.MessageExt) (ConsumeResult, error)
	submitToConsume              func(*ProcessQueue, *kernel.MessageQueue)
	subscribedTopic              map[string]string
}

func NewPushConsumer(consumerGroup string, opt ConsumerOption) PushConsumer {
	dc := &defaultConsumer{
		consumerGroup:  consumerGroup,
		cType:          _PushConsume,
		state:          kernel.StateCreateJust,
		prCh:           make(chan PullRequest, 4),
		model:          opt.ConsumerModel,
		consumeOrderly: opt.ConsumeOrderly,
		fromWhere:      opt.FromWhere,
		option:         opt,
	}

	switch opt.Strategy {
	case StrategyAveragely:
		dc.allocate = allocateByAveragely
	case StrategyAveragelyCircle:
		dc.allocate = allocateByAveragelyCircle
	case StrategyConfig:
		dc.allocate = allocateByConfig
	case StrategyConsistentHash:
		dc.allocate = allocateByConsistentHash
	case StrategyMachineNearby:
		dc.allocate = allocateByMachineNearby
	case StrategyMachineRoom:
		dc.allocate = allocateByMachineRoom
	default:
		dc.allocate = allocateByAveragely
	}

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
	return p
}

func (pc *pushConsumer) Start() error {
	var err error
	pc.once.Do(func() {
		rlog.Infof("the consumerGroup=%s start beginning. messageModel=%v, unitMode=%v",
			pc.consumerGroup, pc.model, pc.unitMode)
		pc.state = kernel.StateStartFailed
		pc.validate()

		if pc.model == Clustering {
			// set retry topic
			retryTopic := kernel.GetRetryTopic(pc.consumerGroup)
			pc.subscriptionDataTable.Store(retryTopic, buildSubscriptionData(retryTopic,
				MessageSelector{TAG, _SubAll}))
		}

		pc.client = kernel.GetOrNewRocketMQClient(pc.option.ClientOption)
		if pc.model == Clustering {
			pc.option.ChangeInstanceNameToPID()
			pc.storage = NewRemoteOffsetStore(pc.consumerGroup)
		} else {
			pc.storage = NewLocalFileOffsetStore(pc.consumerGroup, pc.client.ClientID())
		}
		pc.storage.load()
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
		pc.client.RebalanceImmediately()
		pc.client.Start()
		pc.state = kernel.StateRunning
	})

	pc.client.UpdateTopicRouteInfo()
	pc.client.RebalanceImmediately()
	pc.client.CheckClientInBroker()
	pc.client.SendHeartbeatToAllBrokerWithLock()
	return err
}

func (pc *pushConsumer) Shutdown() {}

func (pc *pushConsumer) Subscribe(topic string, selector MessageSelector,
	f func(*ConsumeMessageContext, []*kernel.MessageExt) (ConsumeResult, error)) error {
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

func (pc *pushConsumer) UpdateTopicSubscribeInfo(topic string, mqs []*kernel.MessageQueue) {
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

func (pc *pushConsumer) messageQueueChanged(topic string, mqAll, mqDivided []*kernel.MessageQueue) {
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
			pc.option.PullBatchSize = 1
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
		rlog.Debugf("pull MessageQueue: %d", request.mq.QueueId)
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
					pc.option.PullThresholdForQueue, 0, pq.msgCache.Front().Value.(int64),
					pq.msgCache.Back().Value.(int64),
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
					pc.option.PullThresholdSizeForQueue, 0, //processQueue.getMsgTreeMap().firstKey(),
					0, // TODO processQueue.getMsgTreeMap().lastKey(),
					pq.msgCache, cachedMessageSizeInMiB, request.String(), pc.queueFlowControlTimes)
			}
			pc.queueFlowControlTimes++
			sleepTime = _PullDelayTimeWhenFlowControl
			goto NEXT
		}

		if !pc.consumeOrderly {
			if pq.getMaxSpan() > pc.option.ConsumeConcurrentlyMaxSpan {

				if pc.queueMaxSpanFlowControlTimes%1000 == 0 {
					rlog.Warnf("the queue's messages, span too long, so do flow control, minOffset=%d, "+
						"maxOffset=%d, maxSpan=%d, pullRequest=%s, flowControlTimes=%d",
						0, //processQueue.getMsgTreeMap().firstKey(),
						0, // processQueue.getMsgTreeMap().lastKey(),
						pq.getMaxSpan(),
						request.String(), pc.queueMaxSpanFlowControlTimes)
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

		if result.Status == kernel.PullBrokerTimeout {
			rlog.Warnf("pull broker: %s timeout", brokerResult.BrokerAddr)
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		switch result.Status {
		case kernel.PullFound:
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
				pq.putMessage(msgFounded)
			}
			if result.NextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset {
				rlog.Warnf("[BUG] pull message result maybe data wrong, [nextBeginOffset=%s, "+
					"firstMsgOffset=%d, prevRequestOffset=%d]", result.NextBeginOffset, firstMsgOffset, prevRequestOffset)
			}
		case kernel.PullNoNewMsg:
			rlog.Infof("Topic: %s, QueueId: %d, no more msg", request.mq.Topic, request.mq.QueueId)
		case kernel.PullNoMsgMatched:
			request.nextOffset = result.NextBeginOffset
			pc.correctTagsOffset(request)
		case kernel.PullOffsetIllegal:
			rlog.Warnf("the pull request offset illegal, {} {}", request.String(), result.String())
			request.nextOffset = result.NextBeginOffset
			pq.dropped = true
			go func() {
				time.Sleep(10 * time.Second)
				pc.storage.update(request.mq, request.nextOffset, false)
				pc.storage.persist([]*kernel.MessageQueue{request.mq})
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

func (pc *pushConsumer) sendMessageBack(ctx *ConsumeMessageContext, msg *kernel.MessageExt) bool {
	return true
}

type ConsumeMessageContext struct {
	consumerGroup string
	msgs          []*kernel.MessageExt
	mq            *kernel.MessageQueue
	success       bool
	status        string
	// mqTractContext
	properties map[string]string
}

func (pc *pushConsumer) consumeMessageCurrently(pq *ProcessQueue, mq *kernel.MessageQueue) {
	msgs := pq.takeMessages(32)
	if msgs == nil {
		return
	}
	for count := 0; count < len(msgs); count++ {
		var subMsgs []*kernel.MessageExt
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

			ctx := &ConsumeMessageContext{
				properties: make(map[string]string),
			}
			// TODO hook
			beginTime := time.Now()
			groupTopic := kernel.RetryGroupTopicPrefix + pc.consumerGroup
			for idx := range subMsgs {
				msg := subMsgs[idx]
				retryTopic := msg.Properties[kernel.PropertyRetryTopic]
				if retryTopic == "" && groupTopic == msg.Topic {
					msg.Topic = retryTopic
				}
				subMsgs[idx].Properties[kernel.PropertyConsumeStartTime] = strconv.FormatInt(
					beginTime.UnixNano()/int64(time.Millisecond), 10)
			}
			result, err := pc.consume(ctx, subMsgs)
			consumeRT := time.Now().Sub(beginTime)
			if err != nil {
				ctx.properties["ConsumeContextType"] = "EXCEPTION"
			} else if consumeRT >= pc.option.ConsumeTimeout {
				ctx.properties["ConsumeContextType"] = "TIMEOUT"
			} else if result == ConsumeSuccess {
				ctx.properties["ConsumeContextType"] = "SUCCESS"
			} else {
				ctx.properties["ConsumeContextType"] = "RECONSUME_LATER"
			}

			// TODO hook
			increaseConsumeRT(pc.consumerGroup, mq.Topic, consumeRT)

			if !pq.dropped {
				msgBackFailed := make([]*kernel.MessageExt, 0)
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
							if !pc.sendMessageBack(ctx, msg) {
								msg.ReconsumeTimes += 1
								msgBackFailed = append(msgBackFailed, msg)
							}
						}
					}
				}

				offset := pq.removeMessage(len(subMsgs))

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

func (pc *pushConsumer) consumeMessageOrderly(pq *ProcessQueue, mq *kernel.MessageQueue) {
}
