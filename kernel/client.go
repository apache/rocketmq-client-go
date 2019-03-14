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
	"github.com/apache/rocketmq-client-go/remote"
	"github.com/apache/rocketmq-client-go/utils"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	defaultTraceRegionID = "DefaultRegion"
	tranceOff            = "false"
)

var (
	log                           = utils.RLog
	namesrvAddrs                  = os.Getenv("rocketmq.namesrv.addr")
	clientIP                      = utils.LocalIP()
	instanceName                  = os.Getenv("rocketmq.client.name")
	pollNameServerInterval        = 30 * time.Second
	heartbeatBrokerInterval       = 30 * time.Second
	persistConsumerOffsetInterval = 5 * time.Second
	unitMode                      = false
	vipChannelEnabled, _          = strconv.ParseBool(os.Getenv("com.rocketmq.sendMessageWithVIPChannel"))
	clientID                      = string(clientIP) + "@" + instanceName
)

var (
	ErrServiceState = errors.New("service state is not Running, please check")
)

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
	DoRebalance()
	PersistConsumerOffset()
	UpdateTopicSubscribeInfo(topic string, mqs []*MessageQueue)
	IsSubscribeTopicNeedUpdate(topic string) bool
	IsUnitMode() bool
}

// SendMessage with batch by sync
func SendMessageSync(ctx context.Context, brokerAddrs, brokerName string, request *SendMessageRequest,
	msgs []*Message) (*SendResult, error) {
	cmd := remote.NewRemotingCommand(SendBatchMessage, request, encodeMessages(msgs))
	response, err := remote.InvokeSync(brokerAddrs, cmd, 3 * time.Second)
	if err != nil {
		log.Warningf("send messages with sync error: %v", err)
		return nil, err
	}

	return processSendResponse(brokerName, msgs, response), nil
}

// SendMessageAsync send message with batch by async
func SendMessageAsync(ctx context.Context, brokerAddrs, brokerName string, request *SendMessageRequest,
	msgs []*Message, f func(result *SendResult)) error {
	return nil
}

func SendMessageOneWay(ctx context.Context, brokerAddrs string, request *SendMessageRequest,
	msgs []*Message) (*SendResult, error) {
	cmd := remote.NewRemotingCommand(SendBatchMessage, request, encodeMessages(msgs))
	err := remote.InvokeOneWay(brokerAddrs, cmd)
	if err != nil {
		log.Warningf("send messages with oneway error: %v", err)
	}
	return nil, err
}

func processSendResponse(brokerName string, msgs []*Message, cmd *remote.RemotingCommand) *SendResult {
	var status SendStatus
	switch cmd.Code {
	case FlushDiskTimeout:
		status = SendFlushDiskTimeout
	case FlushSlaveTimeout:
		status = SendFlushSlaveTimeout
	case SlaveNotAvailable:
		status = SendSlaveNotAvailable
	case Success:
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
		TraceOn:       trace != "" && trace != tranceOff,
	}
}

// PullMessage with sync
func PullMessage(ctx context.Context, brokerAddrs string, request *PullMessageRequest) (*PullResult, error) {
	return nil, nil
}

// PullMessageAsync pull message async
func PullMessageAsync(ctx context.Context, brokerAddrs string, request *PullMessageRequest, f func(result *PullResult)) error {
	return nil
}

// QueryMaxOffset with specific queueId and topic
func QueryMaxOffset(topic string, queueId int) error {
	return nil
}

// QueryConsumerOffset with specific queueId and topic of consumerGroup
func QueryConsumerOffset(consumerGroup, topic string, queue int) (int64, error) {
	return 0, nil
}

// SearchOffsetByTimestamp with specific queueId and topic
func SearchOffsetByTimestamp(topic string, queue int, timestamp int64) (int64, error) {
	return 0, nil
}

// UpdateConsumerOffset with specific queueId and topic
func UpdateConsumerOffset(consumerGroup, topic string, queue int, offset int64) error {
	return nil
}

var (
	// group -> InnerProducer
	producerMap sync.Map

	// group -> InnerConsumer
	consumerMap sync.Map
)

func CheckClientInBroker() {

}

func RegisterConsumer(group string, consumer InnerConsumer) {

}

func UnregisterConsumer(group string) {

}

func RegisterProducer(group string, producer InnerProducer) {

}

func UnregisterProducer(group string) {

}

func SelectProducer(group string) InnerProducer {
	return nil
}

func SelectConsumer(group string) InnerConsumer {
	return nil
}

func encodeMessages(message []*Message) []byte {
	return nil
}

func sendHeartbeatToAllBroker() {

}
