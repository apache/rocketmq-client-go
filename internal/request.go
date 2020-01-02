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

package internal

import (
	"fmt"
	"strconv"
	"time"
)

const (
	ReqSendMessage              = int16(10)
	ReqPullMessage              = int16(11)
	ReqQueryConsumerOffset      = int16(14)
	ReqUpdateConsumerOffset     = int16(15)
	ReqSearchOffsetByTimestamp  = int16(29)
	ReqGetMaxOffset             = int16(30)
	ReqHeartBeat                = int16(34)
	ReqConsumerSendMsgBack      = int16(36)
	ReqENDTransaction           = int16(37)
	ReqGetConsumerListByGroup   = int16(38)
	ReqLockBatchMQ              = int16(41)
	ReqUnlockBatchMQ            = int16(42)
	ReqGetRouteInfoByTopic      = int16(105)
	ReqSendBatchMessage         = int16(320)
	ReqCheckTransactionState    = int16(39)
	ReqNotifyConsumerIdsChanged = int16(40)
	ReqResetConsuemrOffset      = int16(220)
	ReqGetConsumerRunningInfo   = int16(307)
	ReqConsumeMessageDirectly   = int16(309)
)

type SendMessageRequestHeader struct {
	ProducerGroup         string
	Topic                 string
	QueueId               int
	SysFlag               int
	BornTimestamp         int64
	Flag                  int32
	Properties            string
	ReconsumeTimes        int
	UnitMode              bool
	MaxReconsumeTimes     int
	Batch                 bool
	DefaultTopic          string
	DefaultTopicQueueNums string
}

func (request *SendMessageRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["producerGroup"] = request.ProducerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	maps["sysFlag"] = fmt.Sprintf("%d", request.SysFlag)
	maps["bornTimestamp"] = strconv.FormatInt(request.BornTimestamp, 10)
	maps["flag"] = fmt.Sprintf("%d", request.Flag)
	maps["reconsumeTimes"] = strconv.Itoa(request.ReconsumeTimes)
	maps["unitMode"] = strconv.FormatBool(request.UnitMode)
	maps["maxReconsumeTimes"] = strconv.Itoa(request.MaxReconsumeTimes)
	maps["defaultTopic"] = "TBW102"
	maps["defaultTopicQueueNums"] = "4"
	maps["batch"] = strconv.FormatBool(request.Batch)
	maps["properties"] = request.Properties

	return maps
}

type EndTransactionRequestHeader struct {
	ProducerGroup        string
	TranStateTableOffset int64
	CommitLogOffset      int64
	CommitOrRollback     int
	FromTransactionCheck bool
	MsgID                string
	TransactionId        string
}

type SendMessageRequestV2Header struct {
	*SendMessageRequestHeader
}

func (request *SendMessageRequestV2Header) Encode() map[string]string {
	maps := make(map[string]string)
	maps["a"] = request.ProducerGroup
	maps["b"] = request.Topic
	maps["c"] = request.DefaultTopic
	maps["d"] = request.DefaultTopicQueueNums
	maps["e"] = strconv.Itoa(request.QueueId)
	maps["f"] = fmt.Sprintf("%d", request.SysFlag)
	maps["g"] = strconv.FormatInt(request.BornTimestamp, 10)
	maps["h"] = fmt.Sprintf("%d", request.Flag)
	maps["i"] = request.Properties
	maps["j"] = strconv.Itoa(request.ReconsumeTimes)
	maps["k"] = strconv.FormatBool(request.UnitMode)
	maps["l"] = strconv.Itoa(request.MaxReconsumeTimes)
	maps["m"] = strconv.FormatBool(request.Batch)
	return maps
}

func (request *EndTransactionRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["producerGroup"] = request.ProducerGroup
	maps["tranStateTableOffset"] = strconv.FormatInt(request.TranStateTableOffset, 10)
	maps["commitLogOffset"] = strconv.Itoa(int(request.CommitLogOffset))
	maps["commitOrRollback"] = strconv.Itoa(request.CommitOrRollback)
	maps["fromTransactionCheck"] = strconv.FormatBool(request.FromTransactionCheck)
	maps["msgId"] = request.MsgID
	maps["transactionId"] = request.TransactionId
	return maps
}

type CheckTransactionStateRequestHeader struct {
	TranStateTableOffset int64
	CommitLogOffset      int64
	MsgId                string
	TransactionId        string
	OffsetMsgId          string
}

func (request *CheckTransactionStateRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["tranStateTableOffset"] = strconv.FormatInt(request.TranStateTableOffset, 10)
	maps["commitLogOffset"] = strconv.FormatInt(request.CommitLogOffset, 10)
	maps["msgId"] = request.MsgId
	maps["transactionId"] = request.TransactionId
	maps["offsetMsgId"] = request.OffsetMsgId

	return maps
}

func (request *CheckTransactionStateRequestHeader) Decode(properties map[string]string) {
	if len(properties) == 0 {
		return
	}
	if v, existed := properties["tranStateTableOffset"]; existed {
		request.TranStateTableOffset, _ = strconv.ParseInt(v, 10, 0)
	}
	if v, existed := properties["commitLogOffset"]; existed {
		request.CommitLogOffset, _ = strconv.ParseInt(v, 10, 0)
	}
	if v, existed := properties["msgId"]; existed {
		request.MsgId = v
	}
	if v, existed := properties["transactionId"]; existed {
		request.MsgId = v
	}
	if v, existed := properties["offsetMsgId"]; existed {
		request.MsgId = v
	}
}

type ConsumerSendMsgBackRequestHeader struct {
	Group             string
	Offset            int64
	DelayLevel        int
	OriginMsgId       string
	OriginTopic       string
	UnitMode          bool
	MaxReconsumeTimes int32
}

func (request *ConsumerSendMsgBackRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["group"] = request.Group
	maps["offset"] = strconv.FormatInt(request.Offset, 10)
	maps["delayLevel"] = strconv.Itoa(request.DelayLevel)
	maps["originMsgId"] = request.OriginMsgId
	maps["originTopic"] = request.OriginTopic
	maps["unitMode"] = strconv.FormatBool(request.UnitMode)
	maps["maxReconsumeTimes"] = strconv.Itoa(int(request.MaxReconsumeTimes))

	return maps
}

type PullMessageRequestHeader struct {
	ConsumerGroup        string
	Topic                string
	QueueId              int32
	QueueOffset          int64
	MaxMsgNums           int32
	SysFlag              int32
	CommitOffset         int64
	SuspendTimeoutMillis time.Duration
	SubExpression        string
	SubVersion           int64
	ExpressionType       string
}

func (request *PullMessageRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = fmt.Sprintf("%d", request.QueueId)
	maps["queueOffset"] = fmt.Sprintf("%d", request.QueueOffset)
	maps["maxMsgNums"] = fmt.Sprintf("%d", request.MaxMsgNums)
	maps["sysFlag"] = fmt.Sprintf("%d", request.SysFlag)
	maps["commitOffset"] = fmt.Sprintf("%d", request.CommitOffset)
	maps["suspendTimeoutMillis"] = fmt.Sprintf("%d", request.SuspendTimeoutMillis/time.Millisecond)
	maps["subscription"] = request.SubExpression
	maps["subVersion"] = fmt.Sprintf("%d", request.SubVersion)
	maps["expressionType"] = request.ExpressionType

	return maps
}

type GetConsumerListRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

func (request *GetConsumerListRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	return maps
}

type GetMaxOffsetRequestHeader struct {
	Topic   string
	QueueId int
}

func (request *GetMaxOffsetRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	return maps
}

type QueryConsumerOffsetRequestHeader struct {
	ConsumerGroup string
	Topic         string
	QueueId       int
}

func (request *QueryConsumerOffsetRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	return maps
}

type SearchOffsetRequestHeader struct {
	Topic     string
	QueueId   int
	Timestamp int64
}

func (request *SearchOffsetRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	maps["timestamp"] = strconv.FormatInt(request.Timestamp, 10)
	return maps
}

type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string
	Topic         string
	QueueId       int
	CommitOffset  int64
}

func (request *UpdateConsumerOffsetRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	maps["commitOffset"] = strconv.FormatInt(request.CommitOffset, 10)
	return maps
}

type GetRouteInfoRequestHeader struct {
	Topic string
}

func (request *GetRouteInfoRequestHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	return maps
}

type GetConsumerRunningInfoHeader struct {
	consumerGroup string
	clientID      string
}

func (request *GetConsumerRunningInfoHeader) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.consumerGroup
	maps["clientId"] = request.clientID
	return maps
}

func (request *GetConsumerRunningInfoHeader) Decode(properties map[string]string) {
	if len(properties) == 0 {
		return
	}
	if v, existed := properties["consumerGroup"]; existed {
		request.consumerGroup = v
	}

	if v, existed := properties["clientId"]; existed {
		request.clientID = v
	}
}
