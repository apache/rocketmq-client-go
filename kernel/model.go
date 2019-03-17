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
	"fmt"
	"github.com/apache/rocketmq-client-go/utils"
)

// SendStatus of message
type SendStatus int

const (
	SendOK SendStatus = iota
	SendFlushDiskTimeout
	SendFlushSlaveTimeout
	SendSlaveNotAvailable
)

// SendResult RocketMQ send result
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

// PullResult the pull result
type PullResult struct {
	NextBeginOffset      int64
	MinOffset            int64
	MaxOffset            int64
	Status               PullStatus
	SuggestWhichBrokerId int64
	messageBinary        []byte
	messageExts          []*MessageExt
}

func (result *PullResult) GetMessageExts() []*MessageExt {
	if result.messageExts != nil && len(result.messageExts) > 0 {
		return result.messageExts
	}

	return result.messageExts
}

func (result *PullResult) SetMessageExts(msgExts []*MessageExt) {
	result.messageBinary = nil
	result.messageExts = msgExts
}

func (result *PullResult) GetMessages() []*Message {
	if result.messageExts == nil || len(result.messageExts) == 0 {
		return make([]*Message, 0)
	}
	return toMessages(result.messageExts)
}

func toMessages(messageExts []*MessageExt) []*Message {
	msgs := make([]*Message, 0)

	return msgs
}

// MessageQueue message queue
type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int    `json:"queueId"`
}

func (mq *MessageQueue) String() string {
	return fmt.Sprintf("MessageQueue [topic=%s, brokerName=%s, queueId=%d]", mq.Topic, mq.BrokerName, mq.QueueId)
}

func (mq *MessageQueue) HashCode() int {
	result := 1
	result = 31*result + utils.HashString(mq.BrokerName)
	result = 31*result + mq.QueueId
	result = 31*result + utils.HashString(mq.Topic)

	return result
}

type FindBrokerResult struct {
	BrokerAddr    string
	Slave         bool
	BrokerVersion int
}

type (
	// groupName of producer
	producerData string

	consumeType string

	MessageModel     int
	ConsumeFromWhere int
	ServiceState     int
)

const (
	ConsumeActively  = consumeType("PULL")
	ConsumePassively = consumeType("PUSH")

	BroadCasting = MessageModel(1)
	Clustering   = MessageModel(2)

	ConsumeFromLastOffset ConsumeFromWhere = iota
	ConsumeFromFirstOffset
	ConsumeFromTimestamp

	CreateJust ServiceState = iota
	Running
	Shutdown
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

type ExpressionType string

const (
	/**
	 * <ul>
	 * Keywords:
	 * <li>{@code AND, OR, NOT, BETWEEN, IN, TRUE, FALSE, IS, NULL}</li>
	 * </ul>
	 * <p/>
	 * <ul>
	 * Data type:
	 * <li>Boolean, like: TRUE, FALSE</li>
	 * <li>String, like: 'abc'</li>
	 * <li>Decimal, like: 123</li>
	 * <li>Float number, like: 3.1415</li>
	 * </ul>
	 * <p/>
	 * <ul>
	 * Grammar:
	 * <li>{@code AND, OR}</li>
	 * <li>{@code >, >=, <, <=, =}</li>
	 * <li>{@code BETWEEN A AND B}, equals to {@code >=A AND <=B}</li>
	 * <li>{@code NOT BETWEEN A AND B}, equals to {@code >B OR <A}</li>
	 * <li>{@code IN ('a', 'b')}, equals to {@code ='a' OR ='b'}, this operation only support String type.</li>
	 * <li>{@code IS NULL}, {@code IS NOT NULL}, check parameter whether is null, or not.</li>
	 * <li>{@code =TRUE}, {@code =FALSE}, check parameter whether is true, or false.</li>
	 * </ul>
	 * <p/>
	 * <p>
	 * Example:
	 * (a > 10 AND a < 100) OR (b IS NOT NULL AND b=TRUE)
	 * </p>
	 */
	SQL92 = ExpressionType("SQL92")

	/**
	 * Only support or operation such as
	 * "tag1 || tag2 || tag3", <br>
	 * If null or * expression, meaning subscribe all.
	 */
	TAG = ExpressionType("TAG")
)

func IsTagType(exp string) bool {
	if exp == "" || exp == "TAG" {
		return true
	}
	return false
}

var SubAll = "*"

type SubscriptionData struct {
	ClassFilterMode bool
	Topic           string
	SubString       string
	Tags            map[string]bool
	Codes           map[int32]bool
	SubVersion      int64
	ExpType         ExpressionType
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
