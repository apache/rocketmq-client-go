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

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProducer_SendStatus(t *testing.T) {
	assert.Equal(t, "SendOK", SendStatus(int(SendOK)).String())
	assert.Equal(t, "SendFlushDiskTimeout", SendStatus(int(SendFlushDiskTimeout)).String())
	assert.Equal(t, "SendFlushSlaveTimeout", SendStatus(int(SendFlushSlaveTimeout)).String())
	assert.Equal(t, "SendSlaveNotAvailable", SendStatus(int(SendSlaveNotAvailable)).String())
	assert.Equal(t, "Unknown", SendStatus(int(-1)).String())
}

func TestProducer_CreateProducerFailed(t *testing.T) {
	pConfig := ProducerConfig{}

	producer, err := newDefaultProducer(nil)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("config is nil"))
	producer, err = newDefaultProducer(&pConfig)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("GroupId is empty"))
	pConfig.GroupID = "testGroupA"
	producer, err = newDefaultProducer(&pConfig)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("NameServer and NameServerDomain is empty"))
	pConfig.NameServer = "localhost:9876"
	pConfig.ProducerModel = TransProducer
	producer, err = newDefaultProducer(&pConfig)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("ProducerModel is invalid or empty"))
}

func TestProducer_CreateProducer(t *testing.T) {
	pConfig := ProducerConfig{}
	pConfig.GroupID = "testGroupB"
	pConfig.NameServer = "localhost:9876"
	pConfig.InstanceName = "testProducer"
	pConfig.Credentials = &SessionCredentials{
		AccessKey: "AK",
		SecretKey: "SK",
		Channel:   "Cloud"}
	pConfig.LogC = &LogConfig{
		Path:     "/rocketmq/log",
		FileNum:  16,
		FileSize: 1 << 20,
		Level:    LogLevelDebug}
	pConfig.SendMsgTimeout = 30
	pConfig.CompressLevel = 4
	pConfig.MaxMessageSize = 1024
	pConfig.ProducerModel = CommonProducer

	producer, err := newDefaultProducer(&pConfig)
	assert.Nil(t, err)
	assert.NotEmpty(t, producer)
}

func TestDefaultProducer_SendMessageSync(t *testing.T) {
	pConfig := ProducerConfig{}
	pConfig.GroupID = "testGroupSync"
	pConfig.NameServer = "localhost:9876"
	pConfig.ProducerModel = CommonProducer

	producer, err := newDefaultProducer(&pConfig)
	assert.Nil(t, err)
	assert.NotEmpty(t, producer)
	err = producer.Start()
	assert.Nil(t, err)
	msg := &Message{
		Topic: "test",
		Tags:  "TagA",
		Keys:  "Key",
		Body:  "Body1234567890"}
	producer.SendMessageSync(msg)
	//sr, errors := producer.SendMessageSync(msg)
	//assert.Nil(t, errors)
	//assert.NotEmpty(t, sr.MsgId)
	//producer.Shutdown()
}

func TestDefaultProducer_SendMessageOneway(t *testing.T) {
	pConfig := ProducerConfig{}
	pConfig.GroupID = "testGroupOneway"
	pConfig.NameServer = "localhost:9876"
	pConfig.ProducerModel = CommonProducer

	producer, err := newDefaultProducer(&pConfig)
	assert.Nil(t, err)
	assert.NotEmpty(t, producer)
	err = producer.Start()
	assert.Nil(t, err)
	msg := &Message{
		Topic: "test",
		Tags:  "TagA",
		Keys:  "Key",
		Body:  "Body1234567890"}
	producer.SendMessageOneway(msg)
	//errors := producer.SendMessageOneway(msg)
	//assert.Nil(t, errors)
	//producer.Shutdown()
}

func TestDefaultProducer_SendMessageOrderlyByShardingKey(t *testing.T) {
	pConfig := ProducerConfig{}
	pConfig.GroupID = "testGroupOrderlyByKey"
	pConfig.NameServer = "localhost:9876"
	pConfig.ProducerModel = OrderlyProducer

	producer, err := newDefaultProducer(&pConfig)
	assert.Nil(t, err)
	assert.NotEmpty(t, producer)
	err = producer.Start()
	assert.Nil(t, err)
	msg := &Message{
		Topic: "test",
		Tags:  "TagA",
		Keys:  "Key",
		Body:  "Body1234567890"}
	producer.SendMessageOrderlyByShardingKey(msg, "key")
	//sr, errors := producer.SendMessageOrderlyByShardingKey(msg, "key")
	//assert.Nil(t, errors)
	//assert.NotEmpty(t, sr.MsgId)
	//producer.Shutdown()
}

type testMessageQueueSelector struct {
}

func (m *testMessageQueueSelector) Select(size int, msg *Message, arg interface{}) int {
	return 0
}
func TestDefaultProducer_SendMessageOrderly(t *testing.T) {
	pConfig := ProducerConfig{}
	pConfig.GroupID = "testGroupOrderly"
	pConfig.NameServer = "localhost:9876"
	pConfig.ProducerModel = OrderlyProducer

	producer, err := newDefaultProducer(&pConfig)
	assert.Nil(t, err)
	assert.NotEmpty(t, producer)
	err = producer.Start()
	assert.Nil(t, err)
	msg := &Message{
		Topic: "test",
		Tags:  "TagA",
		Keys:  "Key",
		Body:  "Body1234567890"}
	s := &testMessageQueueSelector{}
	producer.SendMessageOrderly(msg, s, nil, 1)
	//sr, errors := producer.SendMessageOrderly(msg, s, nil, 1)
	//assert.Nil(t, errors)
	//assert.NotEmpty(t, sr.MsgId)
	//producer.Shutdown()
}
