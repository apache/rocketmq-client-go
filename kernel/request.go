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
	ReqPullMessage            = int16(11)
	ReqGetConsumerListByGroup = int16(38)
	ReqLockBatchMQ            = int16(41)
	ReqUnlockBatchMQ          = int16(42)
	ReqGetRouteInfoByTopic    = int16(105)
	ReqSendBatchMessage       = int16(320)
)

type SendMessageRequest struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int    `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int    `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int    `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int    `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
	MaxReconsumeTimes     int    `json:"maxReconsumeTimes"`
}

func (request *SendMessageRequest) Encode() map[string]string {
	return nil
}

func (request *SendMessageRequest) Decode(properties map[string]string) error {
	return nil
}

type PullMessageRequest struct {
	ConsumerGroup        string `json:"consumerGroup"`
	Topic                string `json:"topic"`
	QueueId              int32  `json:"queueId"`
	QueueOffset          int64  `json:"queueOffset"`
	MaxMsgNums           int32  `json:"maxMsgNums"`
	SysFlag              int32  `json:"sysFlag"`
	CommitOffset         int64  `json:"commitOffset"`
	SuspendTimeoutMillis int64  `json:"suspendTimeoutMillis"`
	SubExpression        string `json:"subscription"`
	SubVersion           int64  `json:"subVersion"`
	ExpressionType       string `json:"expressionType"`
}

func (request *PullMessageRequest) Encode() map[string]string {
	maps := make(map[string]string)
	maps["consumerGroup"] = request.ConsumerGroup
	maps["topic"] = request.Topic
	maps["queueId"] = fmt.Sprintf("%d", request.QueueOffset)
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
	QueueId int32  `json:"queueId"`
}

type QueryConsumerOffsetRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
}

type SearchOffsetRequest struct {
	Topic     string `json:"topic"`
	QueueId   int32  `json:"queueId"`
	Timestamp int64  `json:"timestamp"`
}

type UpdateConsumerOffsetRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
	CommitOffset  int64  `json:"commitOffset"`
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
