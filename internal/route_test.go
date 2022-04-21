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

package internal

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func TestQueryTopicRouteInfoFromServer(t *testing.T) {
	Convey("marshal of TraceContext", t, func() {

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		remotingCli := remote.NewMockRemotingClient(ctrl)

		addr, err := primitive.NewNamesrvAddr("1.1.1.1:8880", "1.1.1.2:8880", "1.1.1.3:8880")
		assert.Nil(t, err)

		namesrv, err := NewNamesrv(primitive.NewPassthroughResolver(addr), nil)
		assert.Nil(t, err)
		namesrv.nameSrvClient = remotingCli

		Convey("When marshal producer trace data", func() {

			count := 0
			remotingCli.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, addr string, request *remote.RemotingCommand) (*remote.RemotingCommand, error) {
					count++
					if count < 3 {
						return nil, errors.ErrNotExisted
					}
					return &remote.RemotingCommand{
						Code: ResTopicNotExist,
					}, nil
				}).Times(3)

			data, err := namesrv.queryTopicRouteInfoFromServer("notexisted")
			assert.Nil(t, data)
			assert.Equal(t, errors.ErrTopicNotExist, err)
		})
	})
}

func TestAddBrokerVersion(t *testing.T) {
	s := &namesrvs{}
	s.brokerVersionMap = make(map[string]map[string]int32, 0)
	s.brokerLock = new(sync.RWMutex)

	v := s.findBrokerVersion("b1", "addr1")
	assert.Equal(t, v, int32(0))

	s.AddBrokerVersion("b1", "addr1", 1)
	v = s.findBrokerVersion("b1", "addr1")
	assert.Equal(t, v, int32(1))

	v = s.findBrokerVersion("b1", "addr2")
	assert.Equal(t, v, int32(0))
}

func TestFindBrokerAddressInSubscribe(t *testing.T) {
	s := &namesrvs{}
	s.brokerVersionMap = make(map[string]map[string]int32, 0)
	s.brokerLock = new(sync.RWMutex)

	brokerDataRaft1 := &BrokerData{
		Cluster:    "cluster",
		BrokerName: "raft01",
		BrokerAddresses: map[int64]string{
			0: "127.0.0.1:10911",
			1: "127.0.0.1:10912",
			2: "127.0.0.1:10913",
		},
	}
	s.brokerAddressesMap.Store(brokerDataRaft1.BrokerName, brokerDataRaft1)
	brokerDataRaft2 := &BrokerData{
		Cluster:    "cluster",
		BrokerName: "raft02",
		BrokerAddresses: map[int64]string{
			0: "127.0.0.1:10911",
			2: "127.0.0.1:10912",
			3: "127.0.0.1:10913",
		},
	}
	s.brokerAddressesMap.Store(brokerDataRaft2.BrokerName, brokerDataRaft2)

	Convey("Request master broker", t, func() {
		result := s.FindBrokerAddressInSubscribe(brokerDataRaft1.BrokerName, 0, false)
		assert.NotNil(t, result)
		assert.Equal(t, result.BrokerAddr, brokerDataRaft1.BrokerAddresses[0])
		assert.Equal(t, result.Slave, false)
	})

	Convey("Request slave broker from normal broker group", t, func() {
		result := s.FindBrokerAddressInSubscribe(brokerDataRaft1.BrokerName, 1, false)
		assert.NotNil(t, result)
		assert.Equal(t, result.BrokerAddr, brokerDataRaft1.BrokerAddresses[1])
		assert.Equal(t, result.Slave, true)
	})

	Convey("Request slave broker from non normal broker group", t, func() {
		result := s.FindBrokerAddressInSubscribe(brokerDataRaft2.BrokerName, 1, false)
		assert.NotNil(t, result)
		assert.Equal(t, result.BrokerAddr, brokerDataRaft2.BrokerAddresses[2])
		assert.Equal(t, result.Slave, true)
	})

	Convey("Request not exist broker", t, func() {
		result := s.FindBrokerAddressInSubscribe(brokerDataRaft1.BrokerName, 4, false)
		assert.NotNil(t, result)
	})
}
