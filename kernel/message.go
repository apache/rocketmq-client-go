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

import "fmt"

const (
	KeySeparator                   = " "
	Keys                           = "KEYS"
	Tags                           = "TAGS"
	WaitStoreMsgOk                 = "WAIT"
	DelayTimeLevel                 = "DELAY"
	RetryTopic                     = "RETRY_TOPIC"
	RealTopic                      = "REAL_TOPIC"
	RealQueueId                    = "REAL_QID"
	TransactionPrepared            = "TRAN_MSG"
	ProducerGroup                  = "PGROUP"
	MinOffset                      = "MIN_OFFSET"
	MaxOffset                      = "MAX_OFFSET"
	BuyerId                        = "BUYER_ID"
	OriginMessageId                = "ORIGIN_MESSAGE_ID"
	TransferFlag                   = "TRANSFER_FLAG"
	CorrectionFlag                 = "CORRECTION_FLAG"
	MQ2Flag                        = "MQ2_FLAG"
	ReconsumeTime                  = "RECONSUME_TIME"
	MsgRegion                      = "MSG_REGION"
	TraceSwitch                    = "TRACE_ON"
	UniqueClientMessageIdKeyIndex  = "UNIQ_KEY"
	MaxReconsumeTimes              = "MAX_RECONSUME_TIMES"
	ConsumeStartTime               = "CONSUME_START_TIME"
	TranscationPreparedQueueOffset = "TRAN_PREPARED_QUEUE_OFFSET"
	TranscationCheckTimes          = "TRANSACTION_CHECK_TIMES"
	CheckImmunityTimeInSeconds     = "CHECK_IMMUNITY_TIME_IN_SECONDS"
)

type Message struct {
	Topic         string
	Body          []byte
	Flag          int32
	Properties    map[string]string
	TransactionId string
	Batch         bool
}

func NewMessage(topic string, body []byte) *Message {
	return &Message{
		Topic:      topic,
		Body:       body,
		Properties: make(map[string]string),
	}
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
	return msgExt.Properties[Tags]
}

func (msgExt *MessageExt) String() string {
	return fmt.Sprintf("[Message=%s, MsgId=%s, QueueId=%d, StoreSize=%d, QueueOffset=%d, SysFlag=%d, "+
		"BornTimestamp=%d, BornHost=%s, StoreTimestamp=%d, StoreHost=%s, CommitLogOffset=%d, BodyCRC=%d, "+
		"ReconsumeTimes=%d, PreparedTransactionOffset=%d]", msgExt.Message.String(), msgExt.MsgId, msgExt.QueueId,
		msgExt.StoreSize, msgExt.QueueOffset, msgExt.SysFlag, msgExt.BornTimestamp, msgExt.BornHost,
		msgExt.StoreTimestamp, msgExt.StoreHost, msgExt.CommitLogOffset, msgExt.BodyCRC, msgExt.ReconsumeTimes,
		msgExt.PreparedTransactionOffset)
}
