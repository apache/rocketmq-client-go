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

//Version get go sdk version
func Version() (version string) {
	return GetVersion()
}

//ClientConfig save client config
type ClientConfig struct {
	GroupID          string
	NameServer       string
	NameServerDomain string
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
	str = strJoin(str, "InstanceName", config.InstanceName)

	if config.LogC != nil {
		str = strJoin(str, "LogConfig", config.LogC.String())
	}

	return str
}

//ProducerModel Common or orderly
type ProducerModel int

//Different models
const (
	CommonProducer  = ProducerModel(1)
	OrderlyProducer = ProducerModel(2)
	TransProducer   = ProducerModel(3)
)

func (mode ProducerModel) String() string {
	switch mode {
	case CommonProducer:
		return "CommonProducer"
	case OrderlyProducer:
		return "OrderlyProducer"
	case TransProducer:
		return "TransProducer"
	default:
		return "Unknown"
	}
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
	ProducerModel  ProducerModel
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
	str = strJoin(str, "ProducerModel", config.ProducerModel.String())
	return str + "]"
}

//Producer define interface
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

	SendMessageOrderlyByShardingKey(msg *Message, shardingkey string) (*SendResult, error)
}

// NewTransactionProducer create a new  trasaction producer with config
func NewTransactionProducer(config *ProducerConfig, listener TransactionLocalListener, arg interface{}) (TransactionProducer, error) {
	return newDefaultTransactionProducer(config, listener, arg)
}

// TransactionLocalListener local listener for transaction message
type TransactionLocalListener interface {
	Execute(m *Message, arg interface{}) TransactionStatus
	Check(m *MessageExt, arg interface{}) TransactionStatus
}

// TransactionProducer api for send transaction message
type TransactionProducer interface {
	baseAPI
	// send a transaction message with sync
	SendMessageTransaction(msg *Message, arg interface{}) (*SendResult, error)
}

// NewPushConsumer create a new consumer with config.
func NewPushConsumer(config *PushConsumerConfig) (PushConsumer, error) {
	return newPushConsumer(config)
}

//MessageModel Clustering or BroadCasting
type MessageModel int

//MessageModel
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

//ConsumerModel CoCurrently or Orderly
type ConsumerModel int

//ConsumerModel
const (
	CoCurrently = ConsumerModel(1)
	Orderly     = ConsumerModel(2)
)

func (mode ConsumerModel) String() string {
	switch mode {
	case CoCurrently:
		return "CoCurrently"
	case Orderly:
		return "Orderly"
	default:
		return "Unknown"
	}
}

// PushConsumerConfig define a new consumer.
type PushConsumerConfig struct {
	ClientConfig
	ThreadCount             int
	MessageBatchMaxSize     int
	Model                   MessageModel
	ConsumerModel           ConsumerModel
	MaxCacheMessageSize     int
	MaxCacheMessageSizeInMB int
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

	if config.ConsumerModel != 0 {
		str = strJoin(str, "ConsumerModel", config.ConsumerModel.String())
	}

	if config.MaxCacheMessageSize != 0 {
		str = strJoin(str, "MaxCacheMessageSize", config.MaxCacheMessageSize)
	}

	if config.MaxCacheMessageSizeInMB != 0 {
		str = strJoin(str, "MaxCacheMessageSizeInMB", config.MaxCacheMessageSizeInMB)
	}
	return str + "]"
}

// PushConsumer apis for PushConsumer
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

//SessionCredentials access config for client
type SessionCredentials struct {
	AccessKey string
	SecretKey string
	Channel   string
}

func (session *SessionCredentials) String() string {
	return fmt.Sprintf("[accessKey: %s, secretKey: %s, channel: %s]",
		session.AccessKey, session.SecretKey, session.Channel)
}

//SendResult status for send
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
