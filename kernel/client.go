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

package kernel

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/rocketmq-client-go/remote"
	"github.com/apache/rocketmq-client-go/rlog"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	defaultTraceRegionID = "DefaultRegion"

	// tracing message switch
	_TranceOff = "false"

	// Pulling topic information interval from the named server
	_PullNameServerInterval = 30 * time.Second

	// Pulling topic information interval from the named server
	_HeartbeatBrokerInterval = 30 * time.Second
)

var (
	ErrServiceState = errors.New("service state is not running, please check")
)

type ClientOption struct {
	NameServerAddr    string
	ClientIP          string
	InstanceName      string
	UnitMode          bool
	UnitName          string
	VIPChannelEnabled bool
	UseTLS            bool
}

func (opt *ClientOption) ChangeInstanceNameToPID() {
	if opt.InstanceName == "DEFAULT" {
		opt.InstanceName = strconv.Itoa(os.Getegid())
	}
}

func (opt *ClientOption) String() string {
	return fmt.Sprintf("ClientOption [NameServerAddr=%s, ClientIP=%s, InstanceName=%s, "+
		"UnitMode=%v, UnitName=%s, VIPChannelEnabled=%v, UseTLS=%v]", opt.NameServerAddr, opt.ClientIP,
		opt.InstanceName, opt.UnitMode, opt.UnitName, opt.VIPChannelEnabled, opt.UseTLS)
}

type InnerProducer interface {
	PublishTopicList() []string
	UpdateTopicPublishInfo(topic string, info *TopicPublishInfo)
	IsPublishTopicNeedUpdate(topic string) bool
	GetCheckListener() func(msg *MessageExt)
	GetTransactionListener() TransactionListener
	//CheckTransactionState()
	isUnitMode() bool
}

type InnerConsumer interface {
	PersistConsumerOffset()
	UpdateTopicSubscribeInfo(topic string, mqs []*MessageQueue)
	IsSubscribeTopicNeedUpdate(topic string) bool
	IsUnitMode() bool
}

type RMQClient struct {
	option ClientOption
}

func NewRocketMQClient(option ClientOption) *RMQClient {
	return nil
}

func (c *RMQClient) ClientID() string {
	id := c.option.ClientIP + "@" + c.option.InstanceName
	if c.option.UnitName != "" {
		id += "@" + c.option.UnitName
	}
	return id
}

func (c *RMQClient) CheckClientInBroker() {

}

func (c *RMQClient) SendHeartbeatToAllBrokerWithLock() {

}

// SendMessage with batch by sync
func (c *RMQClient) SendMessageSync(ctx context.Context, brokerAddrs, brokerName string, request *SendMessageRequest,
	msgs []*Message) (*SendResult, error) {
	cmd := remote.NewRemotingCommand(ReqSendBatchMessage, request, encodeMessages(msgs))
	response, err := remote.InvokeSync(brokerAddrs, cmd, 3*time.Second)
	if err != nil {
		rlog.Warnf("send messages with sync error: %v", err)
		return nil, err
	}

	return c.processSendResponse(brokerName, msgs, response), nil
}

// SendMessageAsync send message with batch by async
func (c *RMQClient) SendMessageAsync(ctx context.Context, brokerAddrs, brokerName string, request *SendMessageRequest,
	msgs []*Message, f func(result *SendResult)) error {
	return nil
}

func (c *RMQClient) SendMessageOneWay(ctx context.Context, brokerAddrs string, request *SendMessageRequest,
	msgs []*Message) (*SendResult, error) {
	cmd := remote.NewRemotingCommand(ReqSendBatchMessage, request, encodeMessages(msgs))
	err := remote.InvokeOneWay(brokerAddrs, cmd)
	if err != nil {
		rlog.Warnf("send messages with oneway error: %v", err)
	}
	return nil, err
}

func (c *RMQClient) processSendResponse(brokerName string, msgs []*Message, cmd *remote.RemotingCommand) *SendResult {
	var status SendStatus
	switch cmd.Code {
	case ResFlushDiskTimeout:
		status = SendFlushDiskTimeout
	case ResFlushSlaveTimeout:
		status = SendFlushSlaveTimeout
	case ResSlaveNotAvailable:
		status = SendSlaveNotAvailable
	case ResSuccess:
		status = SendOK
	default:
		// TODO process unknown code
	}

	sendResponse := &SendMessageResponse{}
	sendResponse.Decode(cmd.ExtFields)

	msgIDs := make([]string, 0)
	for i := 0; i < len(msgs); i++ {
		msgIDs = append(msgIDs, msgs[i].Properties[UniqueClientMessageIdKeyIndex])
	}

	regionId := cmd.ExtFields[MsgRegion]
	trace := cmd.ExtFields[TraceSwitch]

	if regionId == "" {
		regionId = defaultTraceRegionID
	}

	return &SendResult{
		Status:      status,
		MsgIDs:      msgIDs,
		OffsetMsgID: sendResponse.MsgId,
		MessageQueue: &MessageQueue{
			Topic:      msgs[0].Topic,
			BrokerName: brokerName,
			QueueId:    int(sendResponse.QueueId),
		},
		QueueOffset:   sendResponse.QueueOffset,
		TransactionID: sendResponse.TransactionId,
		RegionID:      regionId,
		TraceOn:       trace != "" && trace != _TranceOff,
	}
}

// PullMessage with sync
func (c *RMQClient) PullMessage(ctx context.Context, brokerAddrs string, request *PullMessageRequest) (*PullResult, error) {
	cmd := remote.NewRemotingCommand(ReqPullMessage, request, nil)

	res, err := remote.InvokeSync(brokerAddrs, cmd, 3*time.Second)
	if err != nil {
		return nil, err
	}

	return c.processPullResponse(res)
}

func (c *RMQClient) processPullResponse(response *remote.RemotingCommand) (*PullResult, error) {
	pullResult := &PullResult{}
	switch response.Code {
	case ResSuccess:
		pullResult.Status = PullFound
	case ResPullNotFound:
		pullResult.Status = PullNoNewMsg
	case ResPullRetryImmediately:
		pullResult.Status = PullNoMsgMatched
	case ResPullOffsetMoved:
		pullResult.Status = PullOffsetIllegal
	default:
		return nil, fmt.Errorf("unknown Response Code: %d, remark: %s", response.Code, response.Remark)
	}

	v, exist := response.ExtFields["maxOffset"]
	if exist {
		pullResult.MaxOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = response.ExtFields["minOffset"]
	if exist {
		pullResult.MinOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = response.ExtFields["nextBeginOffset"]
	if exist {
		pullResult.NextBeginOffset, _ = strconv.ParseInt(v, 10, 64)
	}

	v, exist = response.ExtFields["suggestWhichBrokerId"]
	if exist {
		pullResult.SuggestWhichBrokerId, _ = strconv.ParseInt(v, 10, 64)
	}

	pullResult.messageExts = decodeMessage(response.Body)

	return pullResult, nil
}

// PullMessageAsync pull message async
func (c *RMQClient) PullMessageAsync(ctx context.Context, brokerAddrs string, request *PullMessageRequest, f func(result *PullResult)) error {
	return nil
}

// QueryMaxOffset with specific queueId and topic
func QueryMaxOffset(topic string, queueId int) (int64, error) {
	return 0, nil
}

// QueryConsumerOffset with specific queueId and topic of consumerGroup
func (c *RMQClient) QueryConsumerOffset(consumerGroup, topic string, queue int) (int64, error) {
	return 0, nil
}

// SearchOffsetByTimestamp with specific queueId and topic
func (c *RMQClient) SearchOffsetByTimestamp(topic string, queue int, timestamp int64) (int64, error) {
	return 0, nil
}

// UpdateConsumerOffset with specific queueId and topic
func (c *RMQClient) UpdateConsumerOffset(consumerGroup, topic string, queue int, offset int64) error {
	return nil
}

var (
	// group -> InnerProducer
	producerMap sync.Map

	// group -> InnerConsumer
	consumerMap sync.Map
)

func (c *RMQClient) RegisterConsumer(group string, consumer InnerConsumer) {

}

func (c *RMQClient) UnregisterConsumer(group string) {

}

func (c *RMQClient) RegisterProducer(group string, producer InnerProducer) {

}

func (c *RMQClient) UnregisterProducer(group string) {

}

func (c *RMQClient) SelectProducer(group string) InnerProducer {
	return nil
}

func (c *RMQClient) SelectConsumer(group string) InnerConsumer {
	return nil
}

func encodeMessages(message []*Message) []byte {
	return nil
}
