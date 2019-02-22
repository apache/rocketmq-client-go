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

package common

import (
	log "github.com/sirupsen/logrus"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"github.com/apache/rocketmq-client-go/remote"
	"github.com/pkg/errors"
	"encoding/json"
)

const (
	requestTimeout   = 3000
	defaultTopic     = "TBW102"
	defaultQueueNums = 4
	masterId         = int64(0)
)

var (
	ErrTopicNotExist = errors.New("topic not exist")
)

var (
	brokerAddressesMap sync.Map
	publishInfoMap     sync.Map
	routeDataMap       sync.Map
	lockNamesrv        sync.Mutex
)

// key is topic, value is topicPublishInfo
type topicPublishInfo struct {
	orderTopic          bool
	haveTopicRouterInfo bool
	mqList              []*MessageQueue
	routeData           *topicRouteData
	topicQueueIndex     int32
}

func (info *topicPublishInfo) isOK() (bIsTopicOk bool) {
	return len(info.mqList) > 0
}

func (info *topicPublishInfo) fetchQueueIndex() int {
	length := len(info.mqList)
	if length <= 0 {
		return -1
	}
	qIndex := atomic.AddInt32(&info.topicQueueIndex, 1)
	return int(qIndex) % length
}

func tryToFindTopicPublishInfo(topic string) *topicPublishInfo {
	value, exist := publishInfoMap.Load(topic)

	var info *topicPublishInfo
	if exist {
		info = value.(*topicPublishInfo)
	}

	if info == nil || !info.isOK() {
		updateTopicRouteInfo(topic)
		value, exist = publishInfoMap.Load(topic)
		if !exist {
			info = &topicPublishInfo{haveTopicRouterInfo: false}
		} else {
			info = value.(*topicPublishInfo)
		}
	}

	if info.haveTopicRouterInfo || info.isOK() {
		return info
	}

	value, exist = publishInfoMap.Load(topic)
	if exist {
		return value.(*topicPublishInfo)
	}

	return nil
}

func updateTopicRouteInfo(topic string) {
	// Todo process lock timeout
	lockNamesrv.Lock()
	defer lockNamesrv.Unlock()

	routeData, err := queryTopicRouteInfoFromServer(topic, requestTimeout)
	if err != nil {
		log.Warningf("query topic route from server error: %s", err)
		return
	}

	if routeData == nil {
		log.Warningf("queryTopicRouteInfoFromServer return nil, Topic: %s", topic)
		return
	}

	var changed bool
	oldRouteData, exist := routeDataMap.Load(topic)
	if !exist || routeData == nil {
		changed = true
	} else {
		changed = topicRouteDataIsChange(oldRouteData.(*topicRouteData), routeData)
	}

	if !changed {
		changed = isNeedUpdateTopicRouteInfo(topic)
	} else {
		log.Infof("the topic[%s] route info changed, old[%s] ,new[%s]", topic, oldRouteData, routeData)
	}

	if !changed {
		return
	}

	newTopicRouteData := routeData.clone()

	for _, brokerData := range newTopicRouteData.brokerDataList {
		brokerAddressesMap.Store(brokerData.brokerName, brokerData.brokerAddresses)
	}

	// update publish info
	publishInfo := routeData2PublishInfo(topic, routeData)
	publishInfo.haveTopicRouterInfo = true

	old, _ := publishInfoMap.Load(topic)
	publishInfoMap.Store(topic, publishInfoMap)
	if old != nil {
		log.Infof("Old TopicPublishInfo [%s] removed.", old)
	}
}

func queryTopicRouteInfoFromServer(topic string, timeout time.Duration) (*topicRouteData, error) {
	request := &GetRouteInfoRequest{
		Topic: topic,
	}
	rc := remote.NewRemotingCommand(GetRouteInfoByTopic, request)

	response, err := remote.InvokeSync(getNameServerAddress(), rc, timeout)

	if err != nil {
		return nil, err
	}

	switch response.Code {
	case Success:
		if response.Body == nil {
			return nil, errors.New(response.Remark)
		}
		routeData := &topicRouteData{}
		err = json.Unmarshal(response.Body, routeData)
		if err != nil {
			log.Warningf("unmarshal topicRouteData error: %s", err)
			return nil, err
		}
		return routeData, nil
	case TopicNotExist:
		return nil, ErrTopicNotExist
	default:
		return nil, errors.New(response.Remark)
	}
}


func topicRouteDataIsChange(oldData *topicRouteData, newData *topicRouteData) bool {
	if oldData == nil || newData == nil {
		return true
	}
	oldDataCloned := oldData.clone()
	newDataCloned := newData.clone()

	sort.Slice(oldDataCloned.queueDataList, func(i, j int) bool {
		return strings.Compare(oldDataCloned.queueDataList[i].brokerName, oldDataCloned.queueDataList[j].brokerName) > 0
	})
	sort.Slice(oldDataCloned.brokerDataList, func(i, j int) bool {
		return strings.Compare(oldDataCloned.brokerDataList[i].brokerName, oldDataCloned.brokerDataList[j].brokerName) > 0
	})
	sort.Slice(newDataCloned.queueDataList, func(i, j int) bool {
		return strings.Compare(newDataCloned.queueDataList[i].brokerName, newDataCloned.queueDataList[j].brokerName) > 0
	})
	sort.Slice(newDataCloned.brokerDataList, func(i, j int) bool {
		return strings.Compare(newDataCloned.brokerDataList[i].brokerName, newDataCloned.brokerDataList[j].brokerName) > 0
	})

	return !oldDataCloned.equals(newDataCloned)
}

func isNeedUpdateTopicRouteInfo(topic string) bool {
	value, exist := publishInfoMap.Load(topic)

	return !exist || value.(*topicPublishInfo).isOK()
}

func routeData2PublishInfo(topic string, data *topicRouteData) *topicPublishInfo {
	publishInfo := &topicPublishInfo{
		routeData:  data,
		orderTopic: false,
	}

	if data.orderTopicConf != "" {
		brokers := strings.Split(data.orderTopicConf, ";")
		for _, broker := range brokers {
			item := strings.Split(broker, ":")
			nums, _ := strconv.Atoi(item[1])
			for i := 0; i < nums; i++ {
				mq := &MessageQueue{
					Topic:      topic,
					BrokerName: item[0],
					QueueId:    i,
				}
				publishInfo.mqList = append(publishInfo.mqList, mq)
			}
		}

		publishInfo.orderTopic = true
		return publishInfo
	}

	qds := data.queueDataList
	sort.Slice(qds, func(i, j int) bool {
		return i-j >= 0
	})

	for _, qd := range qds {
		if !isWriteable(qd.perm) {
			continue
		}

		var bData *BrokerData
		for _, bd := range data.brokerDataList {
			if bd.brokerName == qd.brokerName {
				bData = bd
				break
			}
		}

		if bData == nil || bData.brokerAddresses[masterId] == "" {
			continue
		}

		for i := 0; i < qd.writeQueueNums; i++ {
			mq := &MessageQueue{
				Topic:      topic,
				BrokerName: qd.brokerName,
				QueueId:    i,
			}
			publishInfo.mqList = append(publishInfo.mqList, mq)
		}
	}

	return publishInfo
}

func getNameServerAddress() string {
	return ""
}

// topicRouteData topicRouteData
type topicRouteData struct {
	orderTopicConf string
	queueDataList  []*QueueData
	brokerDataList []*BrokerData
}

func (routeData *topicRouteData) clone() *topicRouteData {
	cloned := &topicRouteData{
		orderTopicConf: routeData.orderTopicConf,
		queueDataList: make([]*QueueData, len(routeData.queueDataList)),
		brokerDataList: make([]*BrokerData, len(routeData.brokerDataList)),
	}

	for index, value := range routeData.queueDataList {
		cloned.queueDataList[index] = value
	}

	for index, value := range routeData.brokerDataList {
		cloned.brokerDataList[index] = value
	}

	return cloned
}

func (routeData *topicRouteData) equals(data *topicRouteData) bool {
	return false
}

// QueueData QueueData
type QueueData struct {
	brokerName     string
	readQueueNums  int
	writeQueueNums int
	perm           int
	topicSynFlag   int
}

// BrokerData BrokerData
type BrokerData struct {
	brokerName          string
	brokerAddresses     map[int64]string
	brokerAddressesLock sync.RWMutex
}
