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

import "fmt"

// SendStatus of message
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

type FindBrokerResult struct {
	BrokerAddr    string
	Slave         bool
	BrokerVersion int32
}

type (
	// groupName of producer
	producerData string

	consumeType string

	MessageModel     int
	ConsumeFromWhere int
)

const (
	ConsumeActively  = consumeType("PULL")
	ConsumePassively = consumeType("PUSH")

	BroadCasting = MessageModel(1)
	Clustering   = MessageModel(2)

	ConsumeFromLastOffset ConsumeFromWhere = iota
	ConsumeFromFirstOffset
	ConsumeFromTimestamp
)

func (mode MessageModel) String() string {
	switch mode {
	case BroadCasting:
		return "BroadCasting"
	case Clustering:
		return "Clustering"
	default:
		return "Unknown"
	}
}

type SubscriptionData struct {
}

type consumerData struct {
	groupName         string
	cType             consumeType
	messageModel      MessageModel
	where             ConsumeFromWhere
	subscriptionDatas []SubscriptionData
	unitMode          bool
}

type heartbeatData struct {
	clientId      string
	producerDatas []producerData
	consumerDatas []consumerData
}
