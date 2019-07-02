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
	"github.com/apache/rocketmq-client-go/internal/kernel"
	"github.com/apache/rocketmq-client-go/primitive"
	"strconv"
	"sync"
)

type PullConsumer interface {
	Start()
	Shutdown()
	Pull(ctx context.Context, topic string, selector primitive.MessageSelector, numbers int) (*primitive.PullResult, error)
}

var (
	queueCounterTable sync.Map
)

func NewConsumer(config primitive.ConsumerOption) *defaultPullConsumer {
	return &defaultPullConsumer{
		option: config,
	}
}

type defaultPullConsumer struct {
	state     kernel.ServiceState
	option    primitive.ConsumerOption
	client    *kernel.RMQClient
	GroupName string
	Model     primitive.MessageModel
	UnitMode  bool
}

func (c *defaultPullConsumer) Start() {
	c.state = kernel.StateRunning
}

func (c *defaultPullConsumer) Pull(ctx context.Context, topic string, selector primitive.MessageSelector, numbers int) (*primitive.PullResult, error) {
	mq := getNextQueueOf(topic)
	if mq == nil {
		return nil, fmt.Errorf("prepard to pull topic: %s, but no queue is founded", topic)
	}

	data := buildSubscriptionData(mq.Topic, selector)
	result, err := c.pull(context.Background(), mq, data, c.nextOffsetOf(mq), numbers)

	if err != nil {
		return nil, err
	}

	processPullResult(mq, result, data)
	return result, nil
}

// SubscribeWithChan ack manually
func (c *defaultPullConsumer) SubscribeWithChan(topic, selector primitive.MessageSelector) (chan *primitive.Message, error) {
	return nil, nil
}

// SubscribeWithFunc ack automatic
func (c *defaultPullConsumer) SubscribeWithFunc(topic, selector primitive.MessageSelector,
	f func(msg *primitive.Message) primitive.ConsumeResult) error {
	return nil
}

func (c *defaultPullConsumer) ACK(msg *primitive.Message, result primitive.ConsumeResult) {

}

func (c *defaultPullConsumer) pull(ctx context.Context, mq *primitive.MessageQueue, data *kernel.SubscriptionData,
	offset int64, numbers int) (*primitive.PullResult, error) {
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

	if (data.ExpType == string(primitive.TAG)) && brokerResult.BrokerVersion < kernel.V4_1_0 {
		return nil, fmt.Errorf("the broker [%s, %v] does not upgrade to support for filter message by %v",
			mq.BrokerName, brokerResult.BrokerVersion, data.ExpType)
	}

	sysFlag := buildSysFlag(false, true, true, false)

	if brokerResult.Slave {
		sysFlag = clearCommitOffsetFlag(sysFlag)
	}
	pullRequest := &kernel.PullMessageRequest{
		ConsumerGroup:        c.GroupName,
		Topic:                mq.Topic,
		QueueId:              int32(mq.QueueId),
		QueueOffset:          offset,
		MaxMsgNums:           int32(numbers),
		SysFlag:              sysFlag,
		CommitOffset:         0,
		SuspendTimeoutMillis: _BrokerSuspendMaxTime,
		SubExpression:        data.SubString,
		ExpressionType:       string(data.ExpType),
	}

	if data.ExpType == string(primitive.TAG) {
		pullRequest.SubVersion = 0
	} else {
		pullRequest.SubVersion = data.SubVersion
	}

	// TODO computePullFromWhichFilterServer
	return c.client.PullMessage(ctx, brokerResult.BrokerAddr, pullRequest)
}

func (c *defaultPullConsumer) makeSureStateOK() error {
	if c.state != kernel.StateRunning {
		return fmt.Errorf("the consumer state is [%d], not running", c.state)
	}
	return nil
}

func (c *defaultPullConsumer) subscriptionAutomatically(topic string) {
	// TODO
}

func (c *defaultPullConsumer) nextOffsetOf(queue *primitive.MessageQueue) int64 {
	return 0
}

func processPullResult(mq *primitive.MessageQueue, result *primitive.PullResult, data *kernel.SubscriptionData) {
	updatePullFromWhichNode(mq, result.SuggestWhichBrokerId)
	switch result.Status {
	case primitive.PullFound:
		msgs := result.GetMessageExts()
		msgListFilterAgain := msgs
		if len(data.Tags) > 0 && data.ClassFilterMode {
			msgListFilterAgain = make([]*primitive.MessageExt, len(msgs))
			for _, msg := range msgs {
				_, exist := data.Tags[msg.GetTags()]
				if exist {
					msgListFilterAgain = append(msgListFilterAgain, msg)
				}
			}
		}

		// TODO hook

		for _, msg := range msgListFilterAgain {
			traFlag, _ := strconv.ParseBool(msg.Properties[primitive.PropertyTransactionPrepared])
			if traFlag {
				msg.TransactionId = msg.Properties[primitive.PropertyUniqueClientMessageIdKeyIndex]
			}

			msg.Properties[primitive.PropertyMinOffset] = strconv.FormatInt(result.MinOffset, 10)
			msg.Properties[primitive.PropertyMaxOffset] = strconv.FormatInt(result.MaxOffset, 10)
		}

		result.SetMessageExts(msgListFilterAgain)
	}
}
