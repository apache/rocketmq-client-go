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

package primitive

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/rocketmq-client-go/internal/utils"
)

const (
	PropertyKeySeparator                   = " "
	PropertyKeys                           = "KEYS"
	PropertyTags                           = "TAGS"
	PropertyWaitStoreMsgOk                 = "WAIT"
	PropertyDelayTimeLevel                 = "DELAY"
	PropertyRetryTopic                     = "RETRY_TOPIC"
	PropertyRealTopic                      = "REAL_TOPIC"
	PropertyRealQueueId                    = "REAL_QID"
	PropertyTransactionPrepared            = "TRAN_MSG"
	PropertyProducerGroup                  = "PGROUP"
	PropertyMinOffset                      = "MIN_OFFSET"
	PropertyMaxOffset                      = "MAX_OFFSET"
	PropertyBuyerId                        = "BUYER_ID"
	PropertyOriginMessageId                = "ORIGIN_MESSAGE_ID"
	PropertyTransferFlag                   = "TRANSFER_FLAG"
	PropertyCorrectionFlag                 = "CORRECTION_FLAG"
	PropertyMQ2Flag                        = "MQ2_FLAG"
	PropertyReconsumeTime                  = "RECONSUME_TIME"
	PropertyMsgRegion                      = "MSG_REGION"
	PropertyTraceSwitch                    = "TRACE_ON"
	PropertyUniqueClientMessageIdKeyIndex  = "UNIQ_KEY"
	PropertyMaxReconsumeTimes              = "MAX_RECONSUME_TIMES"
	PropertyConsumeStartTime               = "CONSUME_START_TIME"
	PropertyTranscationPreparedQueueOffset = "TRAN_PREPARED_QUEUE_OFFSET"
	PropertyTranscationCheckTimes          = "TRANSACTION_CHECK_TIMES"
	PropertyCheckImmunityTimeInSeconds     = "CHECK_IMMUNITY_TIME_IN_SECONDS"
)

type Message struct {
	Topic         string
	Body          []byte
	Flag          int32
	Properties    map[string]string
	TransactionId string
	Batch         bool

	// QueueID is the queue that messages will be sent to. the value must be set if want to custom the queue of message,
	// just ignore if not.
	QueueID int
}

func NewMessage(topic string, body []byte) *Message {
	return &Message{
		Topic:      topic,
		Body:       body,
		Properties: make(map[string]string),
	}
}

// SetDelayTimeLevel set message delay time to consume.
// reference delay level definition: 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
// delay level starts from 1. for example, if we set param level=1, then the delay time is 1s.
func (msg *Message) SetDelayTimeLevel(level int) {
	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	msg.Properties[PropertyDelayTimeLevel] = strconv.Itoa(level)
}

func (msg *Message) String() string {
	return fmt.Sprintf("[topic=%s, body=%s, Flag=%d, Properties=%v, TransactionId=%s]",
		msg.Topic, string(msg.Body), msg.Flag, msg.Properties, msg.TransactionId)
}

//
//func (msg *Message) SetTags(tags string) {
//	msg.Properties[tags] = tags
//}

func (msg *Message) PutProperty(key, value string) {
	msg.Properties[key] = value
}

func (msg *Message) RemoveProperty(key string) string {
	value, exist := msg.Properties[key]
	if !exist {
		return ""
	}

	delete(msg.Properties, key)
	return value
}

func (msg *Message) SetKeys(keys []string) {
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteString(PropertyKeySeparator)
	}
	msg.PutProperty(PropertyKeys, sb.String())
}

func (msg *Message) GetTags() string {
	return msg.Properties[PropertyTags]
}

func (msg *Message) GetKeys() string {
	return msg.Properties[PropertyKeys]
}

type MessageExt struct {
	Message
	MsgId                     string
	QueueId                   int32
	StoreSize                 int32
	QueueOffset               int64
	SysFlag                   int32
	BornTimestamp             int64
	BornHost                  string
	StoreTimestamp            int64
	StoreHost                 string
	CommitLogOffset           int64
	BodyCRC                   int32
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
}

func (msgExt *MessageExt) GetTags() string {
	return msgExt.Properties[PropertyTags]
}

func (msgExt *MessageExt) GetRegionID() string {
	return msgExt.Properties[PropertyMsgRegion]
}

func (msgExt *MessageExt) IsTraceOn() string {
	return msgExt.Properties[PropertyTraceSwitch]
}

func (msgExt *MessageExt) String() string {
	return fmt.Sprintf("[Message=%s, MsgId=%s, QueueId=%d, StoreSize=%d, QueueOffset=%d, SysFlag=%d, "+
		"BornTimestamp=%d, BornHost=%s, StoreTimestamp=%d, StoreHost=%s, CommitLogOffset=%d, BodyCRC=%d, "+
		"ReconsumeTimes=%d, PreparedTransactionOffset=%d]", msgExt.Message.String(), msgExt.MsgId, msgExt.QueueId,
		msgExt.StoreSize, msgExt.QueueOffset, msgExt.SysFlag, msgExt.BornTimestamp, msgExt.BornHost,
		msgExt.StoreTimestamp, msgExt.StoreHost, msgExt.CommitLogOffset, msgExt.BodyCRC, msgExt.ReconsumeTimes,
		msgExt.PreparedTransactionOffset)
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

func (mq MessageQueue) Equals(queue *MessageQueue) bool {
	// TODO
	return mq.BrokerName == queue.BrokerName && mq.Topic == queue.Topic && mq.QueueId == mq.QueueId
}

type AccessChannel int

const (
	// connect to private IDC cluster.
	Local AccessChannel = iota
	// connect to Cloud service.
	Cloud
)

type MessageType int

const (
	NormalMsg MessageType = iota
	TransMsgHalf
	TransMsgCommit
	DelayMsg
)
