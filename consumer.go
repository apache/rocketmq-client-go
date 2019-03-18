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

package rocketmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/rocketmq-client-go/kernel"
	"github.com/apache/rocketmq-client-go/rlog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Consumer interface {
	Start()
	Pull(topic, expression string, numbers int) (*kernel.PullResult, error)
	SubscribeWithChan(topic, expression string) (chan *kernel.Message, error)
	SubscribeWithFunc(topic, expression string, f func(msg *kernel.Message) ConsumeResult) error
	ACK(msg *kernel.Message, result ConsumeResult)
}

var (
	queueCounterTable sync.Map
)

type ConsumeResult int

type ConsumerType int

const (
	Original ConsumerType = iota
	Orderly
	Transaction

	SubAll = "*"
)

type ConsumerConfig struct {
	GroupName                  string
	Model                      kernel.MessageModel
	UnitMode                   bool
	MaxReconsumeTimes          int
	PullMessageTimeout         time.Duration
	FromWhere                  kernel.ConsumeFromWhere
	brokerSuspendMaxTimeMillis int64
}

func NewConsumer(config ConsumerConfig) Consumer {
	return &defaultConsumer{
		config: config,
	}
}

type defaultConsumer struct {
	state  kernel.ServiceState
	config ConsumerConfig
}

func (c *defaultConsumer) Start() {
	c.state = kernel.Running
}

func (c *defaultConsumer) Pull(topic, expression string, numbers int) (*kernel.PullResult, error) {
	mq := getNextQueueOf(topic)
	if mq == nil {
		return nil, fmt.Errorf("prepard to pull topic: %s, but no queue is founded", topic)
	}

	data := getSubscriptionData(mq, expression)
	result, err := c.pull(context.Background(), mq, data, c.nextOffsetOf(mq), numbers)

	if err != nil {
		return nil, err
	}

	processPullResult(mq, result, data)
	return result, nil
}

// SubscribeWithChan ack manually
func (c *defaultConsumer) SubscribeWithChan(topic, expression string) (chan *kernel.Message, error) {
	return nil, nil
}

// SubscribeWithFunc ack automatic
func (c *defaultConsumer) SubscribeWithFunc(topic, expression string,
	f func(msg *kernel.Message) ConsumeResult) error {
	return nil
}

func (c *defaultConsumer) ACK(msg *kernel.Message, result ConsumeResult) {

}

func (c *defaultConsumer) pull(ctx context.Context, mq *kernel.MessageQueue, data *kernel.SubscriptionData,
	offset int64, numbers int) (*kernel.PullResult, error) {
	err := c.makeSureStateOK()
	if err != nil {
		return nil, err
	}

	if mq == nil {
		return nil, errors.New("MessageQueue is nil")
	}

	if offset < 0 {
		return nil, errors.New("offset < 0")
	}

	if numbers <= 0 {
		numbers = 1
	}
	c.subscriptionAutomatically(mq.Topic)

	brokerResult := tryFindBroker(mq)
	if brokerResult == nil {
		return nil, fmt.Errorf("the broker %s does not exist", mq.BrokerName)
	}

	if (data.ExpType == kernel.TAG) && brokerResult.BrokerVersion < kernel.V4_1_0 {
		return nil, fmt.Errorf("the broker [%s, %v] does not upgrade to support for filter message by %v",
			mq.BrokerName, brokerResult.BrokerVersion, data.ExpType)
	}

	sysFlag := buildSysFlag(false, true, true, false)

	if brokerResult.Slave {
		sysFlag = clearCommitOffsetFlag(sysFlag)
	}
	pullRequest := &kernel.PullMessageRequest{
		ConsumerGroup:        c.config.GroupName,
		Topic:                mq.Topic,
		QueueId:              int32(mq.QueueId),
		QueueOffset:          offset,
		MaxMsgNums:           int32(numbers),
		SysFlag:              sysFlag,
		CommitOffset:         0,
		SuspendTimeoutMillis: c.config.brokerSuspendMaxTimeMillis,
		SubExpression:        data.SubString,
		ExpressionType:       string(data.ExpType),
	}

	if data.ExpType == kernel.TAG {
		pullRequest.SubVersion = 0
	} else {
		pullRequest.SubVersion = data.SubVersion
	}

	// TODO computePullFromWhichFilterServer
	return kernel.PullMessage(ctx, brokerResult.BrokerAddr, pullRequest)
}

func (c *defaultConsumer) makeSureStateOK() error {
	if c.state != kernel.Running {
		return fmt.Errorf("the consumer state is [%d], not running", c.state)
	}
	return nil
}

func (c *defaultConsumer) subscriptionAutomatically(topic string) {
	// TODO
}

func (c *defaultConsumer) nextOffsetOf(queue *kernel.MessageQueue) int64 {
	return 0
}

func toMessage(messageExts []*kernel.MessageExt) []*kernel.Message {
	msgs := make([]*kernel.Message, 0)

	return msgs
}

func processPullResult(mq *kernel.MessageQueue, result *kernel.PullResult, data *kernel.SubscriptionData) {
	updatePullFromWhichNode(mq, result.SuggestWhichBrokerId)
	switch result.Status {
	case kernel.PullFound:
		msgs := result.GetMessageExts()
		msgListFilterAgain := msgs
		if len(data.Tags) > 0 && data.ClassFilterMode {
			msgListFilterAgain = make([]*kernel.MessageExt, len(msgs))
			for _, msg := range msgs {
				_, exist := data.Tags[msg.GetTags()]
				if exist {
					msgListFilterAgain = append(msgListFilterAgain, msg)
				}
			}
		}

		// TODO hook

		for _, msg := range msgListFilterAgain {
			traFlag, _ := strconv.ParseBool(msg.Properties[kernel.TransactionPrepared])
			if traFlag {
				msg.TransactionId = msg.Properties[kernel.UniqueClientMessageIdKeyIndex]
			}

			msg.Properties[kernel.MinOffset] = strconv.FormatInt(result.MinOffset, 10)
			msg.Properties[kernel.MaxOffset] = strconv.FormatInt(result.MaxOffset, 10)
		}

		result.SetMessageExts(msgListFilterAgain)
	}
}

func getSubscriptionData(mq *kernel.MessageQueue, exp string) *kernel.SubscriptionData {
	subData := &kernel.SubscriptionData{
		Topic: mq.Topic,
	}
	if exp == "" || exp == SubAll {
		subData.SubString = SubAll
	} else {
		// TODO
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
