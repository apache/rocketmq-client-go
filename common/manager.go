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

package common

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/remote"
	"time"
)

const (
	defaultTraceRegionID = "DefaultRegion"
	tranceOff            = "false"
)

// SendMessage with batch by sync
func SendMessageSync(ctx context.Context, brokerAddrs, brokerName string, request *SendMessageRequest,
	msgs []*Message) (*SendResult, error) {
	cmd := remote.NewRemotingCommand(SendBatchMessage, request, encodeMessages(msgs))
	response, err := remote.InvokeSync(brokerAddrs, cmd, 3*time.Second)
	if err != nil {
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
	return nil, err
}

func encodeMessages(message []*Message) []byte {
	return nil
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
		msgIDs = append(msgIDs, msgs[i].Properties[UniqueClientMessageIdKeyindex])

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

//SendStatus message send result
type SendStatus int

const (
	SendOK SendStatus = iota
	SendFlushDiskTimeout
	SendFlushSlaveTimeout
	SendSlaveNotAvailable
)

// SendResult rocketmq send result
type SendResult struct {
	Status        SendStatus
	MsgIDs        []string
	MessageQueue  *MessageQueue
	QueueOffset   int64
	TransactionID string
	OffsetMsgID   string
	RegionID      string
	TraceOn       bool
}

// SendResult send message result to string(detail result)
func (result *SendResult) String() string {
	return fmt.Sprintf("SendResult [sendStatus=%d, msgIds=%s, offsetMsgId=%s, queueOffset=%d, messageQueue=%s]",
		result.Status, result.MsgIDs, result.OffsetMsgID, result.QueueOffset, result.MessageQueue.String())
}

// PullResult the pull result
type PullResult struct {
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	Status          PullStatus
	Messages        []*MessageExt
}

// PullStatus pull status
type PullStatus int

// predefined pull status
const (
	PullFound PullStatus = iota
	PullNoNewMsg
	PullNoMatchedMsg
	PullOffsetIllegal
	PullBrokerTimeout
)

// MessageQueue message queue
type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int    `json:"queueId"`
}

func (mq *MessageQueue) String() string {
	return fmt.Sprintf("MessageQueue [topic=%s, brokerName=%s, queueId=%d]", mq.Topic, mq.BrokerName, mq.QueueId)
}
