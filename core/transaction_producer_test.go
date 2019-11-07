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

func TestTransactionProducer_TransactionStatus(t *testing.T) {
	assert.Equal(t, "CommitTransaction", TransactionStatus(int(CommitTransaction)).String())
	assert.Equal(t, "RollbackTransaction", TransactionStatus(int(RollbackTransaction)).String())
	assert.Equal(t, "UnknownTransaction", TransactionStatus(int(UnknownTransaction)).String())
	assert.Equal(t, "UnknownTransaction", TransactionStatus(int(-1)).String())
}

func TestTransactionProducer_CreateProducerFailed(t *testing.T) {
	pConfig := ProducerConfig{}

	producer, err := newDefaultTransactionProducer(nil, nil, nil)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("config is nil"))
	producer, err = newDefaultTransactionProducer(&pConfig, nil, nil)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("GroupId is empty"))
	pConfig.GroupID = "testGroup"
	producer, err = newDefaultTransactionProducer(&pConfig, nil, nil)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("NameServer and NameServerDomain is empty"))
}

type MyTransactionLocalListener struct {
}

func (l *MyTransactionLocalListener) Execute(m *Message, arg interface{}) TransactionStatus {
	return UnknownTransaction
}
func (l *MyTransactionLocalListener) Check(m *MessageExt, arg interface{}) TransactionStatus {
	return CommitTransaction
}
func TestTransactionProducer_CreateProducer(t *testing.T) {
	pConfig := ProducerConfig{}
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
	pConfig.SendMsgTimeout = 30
	pConfig.CompressLevel = 4
	pConfig.MaxMessageSize = 1024
	listener := &MyTransactionLocalListener{}
	producer, err := newDefaultTransactionProducer(&pConfig, listener, nil)
	assert.Nil(t, err)
	assert.NotEmpty(t, producer)
}
