package consumer

import (
	"context"
	"github.com/apache/rocketmq-client-go/kernel"
	"github.com/apache/rocketmq-client-go/rlog"
	"math"
	"strconv"
	"time"
)

/**
 * In most scenarios, this is the mostly recommended usage to consume messages.
 * </p>
 *
 * Technically speaking, this push client is virtually a wrapper of the underlying pull service. Specifically, on
 * arrival of messages pulled from brokers, it roughly invokes the registered callback handler to feed the messages.
 * </p>
 *
 * See quick start/Consumer in the example module for a typical usage.
 * </p>
 *
 * <p>
 * <strong>Thread Safety:</strong> After initialization, the instance can be regarded as thread-safe.
 * </p>
 */

type ConsumeResult int

const (
	Mb                           = 1024 * 1024
	ConsumeSuccess ConsumeResult = iota
	ConsumeRetryLater
)

type PushConsumer interface {
	Start()
	Shutdown()
	Subscribe(topic, selector MessageSelector, f func(msg *kernel.Message) ConsumeResult) error
}

type pushConsumer struct {
	*defaultConsumer
	queueFlowControlTimes        int
	queueMaxSpanFlowControlTimes int
	consume                      func(*ConsumeMessageContext, []*kernel.MessageExt) (ConsumeResult, error)
	submitToConsume              func([]*kernel.MessageExt, *ProcessQueue, *kernel.MessageQueue, bool)
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
	}
	dc.re = p
	if p.consumeOrderly {
		p.submitToConsume = p.consumeMessageOrderly
	} else {
		p.submitToConsume = p.consumeMessageCurrently
	}
	return p
}

func (pc *pushConsumer) Start() {
	pc.once.Do(func() {
		rlog.Infof("the consumer: %s start beginning. messageModel: %v, unitMode: %v",
			pc.consumerGroup, pc.model, pc.unitMode)
		pc.checkConfig()
		pc.copySubscription()
		pc.client = kernel.NewRocketMQClient(pc.option.ClientOption)

		if pc.model == Clustering {
			pc.changeInstanceNameToPID()
			pc.storage = nil // todo RemoteBrokerOffsetStore{}
		} else {
			pc.storage = nil // LocalFileOffsetStore{}
		}
		pc.storage.load()
		pc.updateTopicSubscribeInfoWhenSubscriptionChanged()
		pc.client.RegisterConsumer(pc.consumerGroup, pc)
		pc.client.CheckClientInBroker()
		pc.client.SendHeartbeatToAllBrokerWithLock()
		//pc.client.RebalanceImmediately()
		// start clean msg expired
		go func() {
			for {
				pr := <-pc.prCh
				// TODO manager
				go pc.pullMessage(&pr)
			}
		}()
	})
}

func (pc *pushConsumer) Shutdown() {}

func (pc *pushConsumer) Subscribe(topic, selector MessageSelector, f func(msg *kernel.Message) ConsumeResult) error {
	return nil
}

func (pc *pushConsumer) DoRebalance() {
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

func (pc *pushConsumer) IsUnitMode() bool {
	return pc.unitMode
}

func (pc *pushConsumer) messageQueueChanged(topic string, mqAll, mqDivided []*kernel.MessageQueue) {

}

func (pc *pushConsumer) updateTopicSubscribeInfoWhenSubscriptionChanged() {

}

func (pc *pushConsumer) checkConfig() {

}

func (pc *pushConsumer) copySubscription() {

}

func (pc *pushConsumer) changeInstanceNameToPID() {

}

func (pc *pushConsumer) pullMessage(request *PullRequest) {
	rlog.Infof("start a nwe Pull Message task %s for [%s]", request.String(), pc.consumerGroup)
	var sleepTime time.Duration
	pq := request.pq
	for {
	NEXT:
		if pq.dropped {
			rlog.Infof("the request: [%s] was dropped, so stop task", request.String())
			return
		}
		if sleepTime > 0 {
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
			rlog.Warnf("consumer [%s] of [%s] was paused, execute pull request [%s] later",
				pc.option.InstanceName, pc.consumerGroup, request.String())
			sleepTime = _PullDelayTimeWhenSuspend
			goto NEXT
		}

		cachedMessageSizeInMiB := int(pq.cachedMsgSize / Mb)
		if pq.cachedMsgCount > pc.option.PullThresholdForQueue {
			if pc.queueFlowControlTimes%1000 == 0 {
				rlog.Warnf("the cached message count exceeds the threshold %d, so do flow control, "+
					"minOffset=%d, maxOffset=%d, count=%d, size=%d MiB, pullRequest=%s, flowControlTimes=%d",
					pc.option.PullThresholdForQueue, 0, //processQueue.getMsgTreeMap().firstKey(),
					0, // TODO processQueue.getMsgTreeMap().lastKey(),
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
		// TODO
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
			ConsumerGroup:        pc.consumerGroup,
			Topic:                request.mq.Topic,
			QueueId:              int32(request.mq.QueueId),
			QueueOffset:          request.nextOffset,
			MaxMsgNums:           pc.option.PullBatchSize,
			SysFlag:              sysFlag,
			CommitOffset:         0,
			SuspendTimeoutMillis: int64(_BrokerSuspendMaxTime),
			SubExpression:        subExpression,
			ExpressionType:       "", // TODO
		}
		//
		//if data.ExpType == string(TAG) {
		//	pullRequest.SubVersion = 0
		//} else {
		//	pullRequest.SubVersion = data.SubVersion
		//}

		ch := make(chan *kernel.PullResult)
		err = pc.client.PullMessageAsync(context.Background(), request.mq.BrokerName, pullRequest, func(result *kernel.PullResult) {
			ch <- result
		})
		if err != nil {
			rlog.Warnf("")
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}
		result := <-ch

		if result.Status == kernel.PullBrokerTimeout {
			rlog.Warnf("")
			sleepTime = _PullDelayTimeWhenError
			goto NEXT
		}

		switch result.Status {
		case kernel.PullFound:
			prevRequestOffset := request.nextOffset
			request.nextOffset = result.NextBeginOffset

			rt := time.Now().Sub(beginTime)
			increasePullRT(pc.consumerGroup, request.mq.Topic, rt)

			msgFounded := result.GetMessageExts()
			firstMsgOffset := int64(math.MaxInt64)
			if msgFounded != nil && len(msgFounded) != 0 {
				firstMsgOffset = msgFounded[0].QueueOffset
				increasePullTPS(pc.consumerGroup, request.mq.Topic, len(msgFounded))
				pc.submitToConsume(msgFounded, pq, request.mq, pq.putMessage(msgFounded))
			}
			if result.NextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset {
				rlog.Warnf("[BUG] pull message result maybe data wrong, [nextBeginOffset=%s, "+
					"firstMsgOffset=%d, prevRequestOffset=%d]", result.NextBeginOffset, firstMsgOffset, prevRequestOffset)
			}
		case kernel.PullNoNewMsg:
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
			rlog.Warnf("")
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

func (pc *pushConsumer) consumeMessageCurrently(msgs []*kernel.MessageExt, pq *ProcessQueue, mq *kernel.MessageQueue,
	dispatchToConsume bool) {
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
		go func() { // TODO max number of goroutine
		RETRY:
			if pq.dropped {
				rlog.Infof("the message queue not be able to consume, because it was dropped. group=%s, mq=%s",
					pc.consumerGroup, mq.String())
				return
			}

			ctx := &ConsumeMessageContext{}
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
					pc.storage.update(mq, offset, true)
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

func (pc *pushConsumer) consumeMessageOrderly(msgs []*kernel.MessageExt, pq *ProcessQueue, mq *kernel.MessageQueue,
	dispatchToConsume bool) {

}
