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

func TestRMQError_OK(t *testing.T) {
	err := rmqError(0)
	assert.Equal(t, NIL, err)
}
func TestRMQError_Failed(t *testing.T) {
	err := rmqError(int(ErrNullPoint))
	assert.Equal(t, ErrNullPoint, err)
}
func TestRMQError_String(t *testing.T) {
	err := rmqError(1)
	expect := "null point"
	assert.Equal(t, expect, err.Error())
}

func TestRMQError_Unknown(t *testing.T) {
	err := rmqError(-1000)
	expect := "unknow error: -1000"
	assert.Equal(t, expect, err.Error())
}

func TestRMQError_ErrorCode(t *testing.T) {
	assert.Equal(t, "malloc memory failed", rmqError(int(ErrMallocFailed)).Error())
	assert.Equal(t, "start producer failed", rmqError(int(ErrProducerStartFailed)).Error())
	assert.Equal(t, "send message with sync failed", rmqError(int(ErrSendSyncFailed)).Error())
	assert.Equal(t, "send message with orderly failed", rmqError(int(ErrSendOrderlyFailed)).Error())
	assert.Equal(t, "send message with oneway failed", rmqError(int(ErrSendOnewayFailed)).Error())
	assert.Equal(t, "send transaction message failed", rmqError(int(ErrSendTransactionFailed)).Error())
	assert.Equal(t, "start push-consumer failed", rmqError(int(ErrPushConsumerStartFailed)).Error())
	assert.Equal(t, "start pull-consumer failed", rmqError(int(ErrPullConsumerStartFailed)).Error())
	assert.Equal(t, "fetch MessageQueue failed", rmqError(int(ErrFetchMQFailed)).Error())
	assert.Equal(t, "fetch Message failed", rmqError(int(ErrFetchMessageFailed)).Error())
	assert.Equal(t, "this function is not support", rmqError(int(ErrNotSupportNow)).Error())
}
