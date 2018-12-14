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

// NewProduer create a new producer with config
func NewProduer(config *ProducerConfig) Producer {
	return newDefaultProducer(config)
}

// ProducerConfig define a producer
type ProducerConfig struct {
	GroupID     string
	NameServer  string
	Credentials *SessionCredentials
}

func (config *ProducerConfig) String() string {
	// For security, don't print Credentials default.
	return fmt.Sprintf("[groupId: %s, nameServer: %s]", config.NameServer, config.GroupID)
}

type Producer interface {
	baseAPI
	// SendMessageSync send a message with sync
	SendMessageSync(msg *Message) SendResult

	// SendMessageOrderly send the message orderly
	SendMessageOrderly(msg *Message, selector MessageQueueSelector, arg interface{}, autoRetryTimes int) SendResult

	// SendMessageAsync send a message with async
	SendMessageAsync(msg *Message)
}

// NewPushConsumer create a new consumer with config.
func NewPushConsumer(config *ConsumerConfig) (PushConsumer, error) {
	return newPushConsumer(config)
}

// ConsumerConfig define a new conusmer.
type ConsumerConfig struct {
	GroupID             string
	NameServer          string
	ConsumerThreadCount int
	MessageBatchMaxSize int
	//ConsumerInstanceName int
	Credentials *SessionCredentials
}

func (config *ConsumerConfig) String() string {
	return fmt.Sprintf("[groupId: %s, nameServer: %s, consumerThreadCount: %d, messageBatchMaxSize: %d]",
		config.GroupID, config.NameServer, config.ConsumerThreadCount, config.MessageBatchMaxSize)
}

type PushConsumer interface {
	baseAPI
	// Subscribe a new topic with specify filter expression and consume function.
	Subscribe(topic, expression string, consumeFunc func(msg *MessageExt) ConsumeStatus) error
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

func (result SendResult) String() string {
	return fmt.Sprintf("[status: %s, messageId: %s, offset: %d]", result.Status, result.MsgId, result.Offset)

}

type baseAPI interface {
	Start() error
	Shutdown() error
}
