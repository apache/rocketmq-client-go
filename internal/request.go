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
	ReqSearchOffsetByTimestamp  = int16(30)
	ReqGetMaxOffset             = int16(30)
	ReqHeartBeat                = int16(34)
	ReqConsumerSendMsgBack      = int16(36)
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

type SendMessageRequest struct {
	ProducerGroup     string `json:"producerGroup"`
	Topic             string `json:"topic"`
	QueueId           int    `json:"queueId"`
	SysFlag           int    `json:"sysFlag"`
	BornTimestamp     int64  `json:"bornTimestamp"`
	Flag              int32  `json:"flag"`
	Properties        string `json:"properties"`
	ReconsumeTimes    int    `json:"reconsumeTimes"`
	UnitMode          bool   `json:"unitMode"`
	MaxReconsumeTimes int    `json:"maxReconsumeTimes"`
	Batch             bool
}

func (request *SendMessageRequest) Encode() map[string]string {
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

func (request *SendMessageRequest) Decode(properties map[string]string) error {
	return nil
}

type ConsumerSendMsgBackRequest struct {
	Group             string `json:"group"`
	Offset            int64  `json:"offset"`
	DelayLevel        int    `json:"delayLevel"`
	OriginMsgId       string `json:"originMsgId"`
	OriginTopic       string `json:"originTopic"`
	UnitMode          bool   `json:"unitMode"`
	MaxReconsumeTimes int32  `json:"maxReconsumeTimes"`
}

func (request *ConsumerSendMsgBackRequest) Encode() map[string]string {
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

type PullMessageRequest struct {
	ConsumerGroup        string        `json:"consumerGroup"`
	Topic                string        `json:"topic"`
	QueueId              int32         `json:"queueId"`
	QueueOffset          int64         `json:"queueOffset"`
	MaxMsgNums           int32         `json:"maxMsgNums"`
	SysFlag              int32         `json:"sysFlag"`
	CommitOffset         int64         `json:"commitOffset"`
	SuspendTimeoutMillis time.Duration `json:"suspendTimeoutMillis"`
	SubExpression        string        `json:"subscription"`
	SubVersion           int64         `json:"subVersion"`
	ExpressionType       string        `json:"expressionType"`
}

func (request *PullMessageRequest) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = fmt.Sprintf("%d", request.QueueId)
	maps["queueOffset"] = fmt.Sprintf("%d", request.QueueOffset)
	maps["maxMsgNums"] = fmt.Sprintf("%d", request.MaxMsgNums)
	maps["sysFlag"] = fmt.Sprintf("%d", request.SysFlag)
	maps["commitOffset"] = fmt.Sprintf("%d", request.CommitOffset)
	maps["suspendTimeoutMillis"] = fmt.Sprintf("%d", request.SuspendTimeoutMillis)
	maps["subscription"] = request.SubExpression
	maps["subVersion"] = fmt.Sprintf("%d", request.SubVersion)
	maps["expressionType"] = request.ExpressionType
	return maps
}

type GetConsumerList struct {
	ConsumerGroup string `json:"consumerGroup"`
}

func (request *GetConsumerList) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	return maps
}

type GetMaxOffsetRequest struct {
	Topic   string `json:"topic"`
	QueueId int    `json:"queueId"`
}

func (request *GetMaxOffsetRequest) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	return maps
}

type QueryConsumerOffsetRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int    `json:"queueId"`
}

func (request *QueryConsumerOffsetRequest) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	return maps
}

type SearchOffsetRequest struct {
	Topic     string `json:"topic"`
	QueueId   int    `json:"queueId"`
	Timestamp int64  `json:"timestamp"`
}

func (request *SearchOffsetRequest) Encode() map[string]string {
	maps := make(map[string]string)
	maps["Topic"] = request.Topic
	maps["QueueId"] = strconv.Itoa(request.QueueId)
	maps["timestamp"] = strconv.FormatInt(request.Timestamp, 10)
	return maps
}

type UpdateConsumerOffsetRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int    `json:"queueId"`
	CommitOffset  int64  `json:"commitOffset"`
}

func (request *UpdateConsumerOffsetRequest) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = strconv.Itoa(request.QueueId)
	maps["commitOffset"] = strconv.FormatInt(request.CommitOffset, 10)
	return maps
}

type GetRouteInfoRequest struct {
	Topic string `json:"topic"`
}

func (request *GetRouteInfoRequest) Encode() map[string]string {
	maps := make(map[string]string)
	maps["topic"] = request.Topic
	return maps
}

func (request *GetRouteInfoRequest) Decode(properties map[string]string) error {
	return nil
}
