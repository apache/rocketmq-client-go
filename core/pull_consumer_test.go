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

func TestPullConsumer_PullStatus(t *testing.T) {
	assert.Equal(t, "Found", PullStatus(int(PullFound)).String())
	assert.Equal(t, "NoNewMsg", PullStatus(int(PullNoNewMsg)).String())
	assert.Equal(t, "NoMatchedMsg", PullStatus(int(PullNoMatchedMsg)).String())
	assert.Equal(t, "OffsetIllegal", PullStatus(int(PullOffsetIllegal)).String())
	assert.Equal(t, "BrokerTimeout", PullStatus(int(PullBrokerTimeout)).String())
	assert.Equal(t, "Unknown status", PullStatus(int(-1)).String())
}

func TestPullConsumer_CreatePullCosumerFailed(t *testing.T) {
	pConfig := PullConsumerConfig{}

	producer, err := NewPullConsumer(nil)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("config is nil"))
	producer, err = NewPullConsumer(&pConfig)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("GroupId is empty"))
	pConfig.GroupID = "testGroup"
	producer, err = NewPullConsumer(&pConfig)
	assert.Nil(t, producer)
	assert.Equal(t, err, errors.New("NameServer and NameServerDomain is empty"))
}
