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

func TestPushConsumer_ConsumeStatus(t *testing.T) {
	assert.Equal(t, "ConsumeSuccess", ConsumeStatus(int(ConsumeSuccess)).String())
	assert.Equal(t, "ReConsumeLater", ConsumeStatus(int(ReConsumeLater)).String())
	assert.Equal(t, "Unknown", ConsumeStatus(int(-1)).String())
}

func TestPushConsumer_CreatePushConsumerFailed(t *testing.T) {
	pConfig := PushConsumerConfig{}

	consumer, err := newPushConsumer(nil)
	assert.Nil(t, consumer)
	assert.Equal(t, err, errors.New("config is nil"))
	consumer, err = newPushConsumer(&pConfig)
	assert.Nil(t, consumer)
	assert.Equal(t, err, errors.New("GroupId is empty"))
	pConfig.GroupID = "testGroupFailedA"
	consumer, err = newPushConsumer(&pConfig)
	assert.Nil(t, consumer)
	assert.Equal(t, err, errors.New("NameServer and NameServerDomain is empty"))
	pConfig.NameServer = "localhost:9876"
	consumer, err = newPushConsumer(&pConfig)
	assert.Nil(t, consumer)
	assert.Equal(t, err, errors.New("model is invalid or empty"))
	pConfig.Model = Clustering
	consumer, err = newPushConsumer(&pConfig)
	assert.Nil(t, consumer)
	assert.Equal(t, err, errors.New("consumer model is invalid or empty"))
	//pConfig.ConsumerModel = CoCurrently
	//pConfig.MaxCacheMessageSizeInMB = 1024
	//consumer, err = newPushConsumer(&pConfig)
	//assert.Nil(t, err)
	//assert.NotNil(t, consumer)
}

func TestPushConsumer_CreatePushConsumer(t *testing.T) {
	pConfig := PushConsumerConfig{}
	pConfig.GroupID = "testGroupSuccessA"
	pConfig.NameServer = "localhost:9876"
	pConfig.InstanceName = "testProducerA"
	pConfig.Credentials = &SessionCredentials{
		AccessKey: "AK",
		SecretKey: "SK",
		Channel:   "Cloud"}
	pConfig.LogC = &LogConfig{
		Path:     "/rocketmq/log",
		FileNum:  16,
		FileSize: 1 << 20,
		Level:    LogLevelDebug}
	pConfig.ConsumerModel = CoCurrently
	pConfig.Model = Clustering
	pConfig.ThreadCount = 3
	pConfig.MessageBatchMaxSize = 1
	pConfig.MaxCacheMessageSize = 1000
	//pConfig.MaxCacheMessageSizeInMB = 1024
	consumer, err := newPushConsumer(&pConfig)
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
}
func callbackTest(msg *MessageExt) ConsumeStatus {
	return ReConsumeLater
}
func TestPushConsumer_CreatePushConsumerSubscribe(t *testing.T) {
	pConfig := PushConsumerConfig{}
	pConfig.GroupID = "testGroup"
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
	pConfig.ConsumerModel = CoCurrently
	pConfig.Model = Clustering
	pConfig.ThreadCount = 3
	pConfig.MessageBatchMaxSize = 1
	pConfig.MaxCacheMessageSize = 1000
	//pConfig.MaxCacheMessageSizeInMB = 1024
	consumer, err := newPushConsumer(&pConfig)
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	err = consumer.Subscribe("Topic", "exp", nil)
	assert.Equal(t, err, errors.New("consumeFunc is nil"))
	err = consumer.Subscribe("Topic", "exp", callbackTest)
	assert.Nil(t, err)
}
