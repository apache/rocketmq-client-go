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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProducerConfig_String(t *testing.T) {
	pConfig := ProducerConfig{}
	pConfig.GroupID = "testGroup"
	pConfig.NameServer = "localhost:9876"
	pConfig.NameServerDomain = "domain1"
	pConfig.InstanceName = "testProducer"
	pConfig.LogC = &LogConfig{
		Path:     "/rocketmq/log",
		FileNum:  16,
		FileSize: 1 << 20,
		Level:    LogLevelDebug}
	pConfig.SendMsgTimeout = 30
	pConfig.CompressLevel = 4
	pConfig.MaxMessageSize = 1024
	pConfig.ProducerModel = CommonProducer

	expect := "ProducerConfig=[GroupId: testGroup, NameServer: localhost:9876, NameServerDomain: domain1, InstanceName: testProducer, " +
		"LogConfig: {Path:/rocketmq/log FileNum:16 FileSize:1048576 Level:Debug}, " +
		"SendMsgTimeout: 30, CompressLevel: 4, MaxMessageSize: 1024, ProducerModel: CommonProducer, ]"
	assert.Equal(t, expect, pConfig.String())
}

func TestPushConsumerConfig_String(t *testing.T) {
	pcConfig := PushConsumerConfig{}
	pcConfig.GroupID = "testGroup"
	pcConfig.NameServer = "localhost:9876"
	pcConfig.InstanceName = "testPushConsumer"
	pcConfig.LogC = &LogConfig{
		Path:     "/rocketmq/log",
		FileNum:  16,
		FileSize: 1 << 20,
		Level:    LogLevelDebug}
	pcConfig.ThreadCount = 4
	pcConfig.MessageBatchMaxSize = 1024
	expect := "PushConsumerConfig=[GroupId: testGroup, NameServer: localhost:9876, " +
		"InstanceName: testPushConsumer, LogConfig: {Path:/rocketmq/log FileNum:16 FileSize:1048576 Level:Debug}, " +
		"ThreadCount: 4, MessageBatchMaxSize: 1024, ]"
	assert.Equal(t, expect, pcConfig.String())

	pcConfig.NameServerDomain = "domain1"
	expect = "PushConsumerConfig=[GroupId: testGroup, NameServer: localhost:9876, NameServerDomain: domain1, InstanceName: testPushConsumer, " +
		"LogConfig: {Path:/rocketmq/log FileNum:16 FileSize:1048576 Level:Debug}, ThreadCount: 4, MessageBatchMaxSize: 1024, ]"
	assert.Equal(t, expect, pcConfig.String())

	pcConfig.MessageBatchMaxSize = 32
	pcConfig.Model = Clustering
	pcConfig.ConsumerModel = CoCurrently
	pcConfig.MaxCacheMessageSize = 1024
	pcConfig.MaxCacheMessageSizeInMB = 2048
	expect = "PushConsumerConfig=[GroupId: testGroup, NameServer: localhost:9876, NameServerDomain: domain1, InstanceName: testPushConsumer, " +
		"LogConfig: {Path:/rocketmq/log FileNum:16 FileSize:1048576 Level:Debug}, ThreadCount: 4," +
		" MessageBatchMaxSize: 32, MessageModel: Clustering, ConsumerModel: CoCurrently," +
		" MaxCacheMessageSize: 1024, MaxCacheMessageSizeInMB: 2048, ]"
	assert.Equal(t, expect, pcConfig.String())
}

func TestPullConfig_String(t *testing.T) {
	pConfig := PullConsumerConfig{}
	pConfig.GroupID = "testGroup"
	pConfig.NameServer = "localhost:9876"
	pConfig.NameServerDomain = "domain1"
	pConfig.InstanceName = "testProducer"
	pConfig.LogC = &LogConfig{
		Path:     "/rocketmq/log",
		FileNum:  16,
		FileSize: 1 << 20,
		Level:    LogLevelDebug}

	expect := "PushConsumerConfig=[GroupId: testGroup, NameServer: localhost:9876, NameServerDomain: domain1, InstanceName: testProducer, " +
		"LogConfig: {Path:/rocketmq/log FileNum:16 FileSize:1048576 Level:Debug}, ]"
	assert.Equal(t, expect, pConfig.String())
}

func TestSessionCredentials_String(t *testing.T) {
	pConfig := SessionCredentials{}
	pConfig.AccessKey = "AK"
	pConfig.SecretKey = "SK"
	pConfig.Channel = "Cloud"

	expect := "[accessKey: AK, secretKey: SK, channel: Cloud]"
	assert.Equal(t, expect, pConfig.String())
}

func TestSendResult_String(t *testing.T) {
	pConfig := SendResult{}
	pConfig.Status = SendOK
	pConfig.MsgId = "MessageId"
	pConfig.Offset = 100000

	expect := "[status: SendOK, messageId: MessageId, offset: 100000]"
	assert.Equal(t, expect, pConfig.String())

	pConfig.Status = SendFlushDiskTimeout
	expect = "[status: SendFlushDiskTimeout, messageId: MessageId, offset: 100000]"
	assert.Equal(t, expect, pConfig.String())
}
