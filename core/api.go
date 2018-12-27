/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package rocketmq

import "fmt"

func Version() (version string) {
	return GetVersion()
}

type ClientConfig struct {
	GroupID          string
	NameServer       string
	NameServerDomain string
	GroupName        string
	InstanceName     string
	Credentials      *SessionCredentials
	LogC             *LogConfig
}

func (config *ClientConfig) String() string {
	// For security, don't print Credentials.
	str := ""
	str = strJoin(str, "GroupId", config.GroupID)
	str = strJoin(str, "NameServer", config.NameServer)
	str = strJoin(str, "NameServerDomain", config.NameServerDomain)
	str = strJoin(str, "GroupName", config.GroupName)
	str = strJoin(str, "InstanceName", config.InstanceName)

	if config.LogC != nil {
		str = strJoin(str, "LogConfig", config.LogC.String())
	}

	return str
}

// NewProducer create a new producer with config
func NewProducer(config *ProducerConfig) (Producer, error) {
	return newDefaultProducer(config)
}

// ProducerConfig define a producer
type ProducerConfig struct {
	ClientConfig
	SendMsgTimeout int
	CompressLevel  int
	MaxMessageSize int
}

func (config *ProducerConfig) String() string {
	str := "ProducerConfig=[" + config.ClientConfig.String()

	if config.SendMsgTimeout > 0 {
		str = strJoin(str, "SendMsgTimeout", config.SendMsgTimeout)
	}

	if config.CompressLevel > 0 {
		str = strJoin(str, "CompressLevel", config.CompressLevel)
	}

	if config.MaxMessageSize > 0 {
		str = strJoin(str, "MaxMessageSize", config.MaxMessageSize)
	}

	return str + "]"
}

type Producer interface {
	baseAPI
	// SendMessageSync send a message with sync
	SendMessageSync(msg *Message) (*SendResult, error)

	// SendMessageOrderly send the message orderly
	SendMessageOrderly(
		msg *Message,
		selector MessageQueueSelector,
		arg interface{},
		autoRetryTimes int) (*SendResult, error)

	// SendMessageOneway send a message with oneway
	SendMessageOneway(msg *Message) error
}

// NewPushConsumer create a new consumer with config.
func NewPushConsumer(config *PushConsumerConfig) (PushConsumer, error) {
	return newPushConsumer(config)
}

type MessageModel int

const (
	BroadCasting = MessageModel(1)
	Clustering   = MessageModel(2)
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

// PushConsumerConfig define a new consumer.
type PushConsumerConfig struct {
	ClientConfig
	ThreadCount         int
	MessageBatchMaxSize int
	Model               MessageModel
}

func (config *PushConsumerConfig) String() string {
	// For security, don't print Credentials.
	str := "PushConsumerConfig=[" + config.ClientConfig.String()

	if config.ThreadCount > 0 {
		str = strJoin(str, "ThreadCount", config.ThreadCount)
	}

	if config.MessageBatchMaxSize > 0 {
		str = strJoin(str, "MessageBatchMaxSize", config.MessageBatchMaxSize)
	}

	if config.Model != 0 {
		str = strJoin(str, "MessageModel", config.Model.String())
	}

	return str + "]"
}

type PushConsumer interface {
	baseAPI

	// Subscribe a new topic with specify filter expression and consume function.
	Subscribe(topic, expression string, consumeFunc func(msg *MessageExt) ConsumeStatus) error
}

// PullConsumerConfig the configuration for the pull consumer
type PullConsumerConfig struct {
	ClientConfig
}

func (config *PullConsumerConfig) String() string {
	return "PushConsumerConfig=[" + config.ClientConfig.String() + "]"
}

// PullConsumer consumer pulling the message
type PullConsumer interface {
	baseAPI

	// Pull returns the messages from the consume queue by specify the offset and the max number
	Pull(mq MessageQueue, subExpression string, offset int64, maxNums int) PullResult

	// FetchSubscriptionMessageQueues returns the consume queue of the topic
	FetchSubscriptionMessageQueues(topic string) []MessageQueue
}

type SessionCredentials struct {
	AccessKey string
	SecretKey string
	Channel   string
}

func (session *SessionCredentials) String() string {
	return fmt.Sprintf("[accessKey: %s, secretKey: %s, channel: %s]",
		session.AccessKey, session.SecretKey, session.Channel)
}

type SendResult struct {
	Status SendStatus
	MsgId  string
	Offset int64
}

func (result *SendResult) String() string {
	return fmt.Sprintf("[status: %s, messageId: %s, offset: %d]", result.Status, result.MsgId, result.Offset)
}

type baseAPI interface {
	Start() error
	Shutdown() error
}
