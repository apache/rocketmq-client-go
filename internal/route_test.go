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
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

const (
	topic = "TopicTest"
)

func init() {
	srvs := []string{"127.0.0.1:9876"}
	namesrv, err := NewNamesrv(srvs...)
	if err != nil {
		panic("register namesrv fail")
	}
	RegisterNamsrv(namesrv)
}

func TestAddBroker(t *testing.T) {
	Convey("Given a starting topic", t, func() {
		remoteRouteData, err := queryTopicRouteInfoFromServer(topic)
		So(err, ShouldBeNil)
		AddBroker(remoteRouteData)

		Convey("brokerData from brokerAddressesMap by brokeName should be deep equal remoteBrokeData from server", func() {
			for _, remoteBrokerData := range remoteRouteData.BrokerDataList {
				brokerName := remoteBrokerData.BrokerName
				brokerData, ok := brokerAddressesMap.Load(brokerName)
				So(ok, ShouldBeTrue)
				So(brokerData, ShouldResemble, remoteBrokerData)
			}
		})
	})
}

func TestUpdateTopicRouteInfo(t *testing.T) {
	Convey("Given a starting topic", t, func() {
		updatedRouteData := UpdateTopicRouteInfo(topic)

		Convey("updatedRouteData should be deep equal remoteRouteData", func() {
			remoteRouteData, err := queryTopicRouteInfoFromServer(topic)
			So(err, ShouldBeNil)
			So(updatedRouteData, ShouldResemble, remoteRouteData)
		})
		Convey("updatedRouteData should be deep equal localRouteData", func() {
			localRouteData, exist := routeDataMap.Load(topic)
			So(exist, ShouldBeTrue)
			So(updatedRouteData, ShouldResemble, localRouteData)
		})
	})
}

func TestFindBrokerAddrByTopic(t *testing.T) {
	Convey("Given a starting topic", t, func() {
		addr := FindBrokerAddrByTopic(topic)
		remoteRouteData, err := queryTopicRouteInfoFromServer(topic)
		So(err, ShouldBeNil)
		brokerAddrList := remoteRouteData.BrokerDataList

		Convey("addr from FindBrokerAddrByTopic should be contained in remoteRouteData", func() {
			flag := false
			for _, brokerData := range brokerAddrList {
				for _, ba := range brokerData.BrokerAddresses {
					if ba == addr {
						flag = true
						break
					}
				}
			}
			So(flag, ShouldBeTrue)
		})
	})
}

func TestFindBrokerAddrByName(t *testing.T) {
	Convey("Given a starting topic", t, func() {
		remoteRouteData, err := queryTopicRouteInfoFromServer(topic)
		So(err, ShouldBeNil)
		brokerAddrList := remoteRouteData.BrokerDataList

		Convey("addr from FindBrokerAddrByName should be equal remoteBrokerAddr from server", func() {
			for _, brokerData := range brokerAddrList {
				brokerName := brokerData.BrokerName
				addr := FindBrokerAddrByName(brokerName)
				remoteBrokerAddr := brokerData.BrokerAddresses[MasterId]
				So(addr, ShouldEqual, remoteBrokerAddr)
			}
		})
	})
}

func TestFindBrokerAddressInSubscribe(t *testing.T) {
	Convey("Given a starting topic", t, func() {
		remoteRouteData, err := queryTopicRouteInfoFromServer(topic)
		So(err, ShouldBeNil)
		brokerAddrList := remoteRouteData.BrokerDataList

		Convey("range BrokerAddress and compare them in turn", func() {
			for _, brokerData := range brokerAddrList {
				brokerName := brokerData.BrokerName
				for id, ba := range brokerData.BrokerAddresses {
					findBrokerRes := FindBrokerAddressInSubscribe(brokerName, id, true)
					res := &FindBrokerResult{
						BrokerAddr:    ba,
						Slave:         false,
						BrokerVersion: findBrokerVersion(brokerName, ba),
					}
					if id != MasterId {
						res.Slave = true
					}
					So(findBrokerRes, ShouldResemble, res)
				}
			}
		})
	})
}
