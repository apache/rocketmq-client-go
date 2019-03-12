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

package kernel

import (
	"encoding/json"
	"errors"
	"github.com/apache/rocketmq-client-go/remote"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	requestTimeout   = 3000
	defaultTopic     = "TBW102"
	defaultQueueNums = 4
	MasterId         = int64(0)
)

var (
	ErrTopicNotExist = errors.New("topic not exist")
)

var (
	// brokerName -> *BrokerData
	brokerAddressesMap sync.Map

	// brokerName -> map[string]int32
	brokerVersionMap sync.Map

	publishInfoMap sync.Map
	routeDataMap   sync.Map
	lockNamesrv    sync.Mutex
)

// key is topic, value is TopicPublishInfo
type TopicPublishInfo struct {
	OrderTopic          bool
	HaveTopicRouterInfo bool
	MqList              []*MessageQueue
	RouteData           *topicRouteData
	TopicQueueIndex     int32
}

func (info *TopicPublishInfo) isOK() (bIsTopicOk bool) {
	return len(info.MqList) > 0
}

func (info *TopicPublishInfo) fetchQueueIndex() int {
	length := len(info.MqList)
	if length <= 0 {
		return -1
	}
	qIndex := atomic.AddInt32(&info.TopicQueueIndex, 1)
	return int(qIndex) % length
}

func UpdateTopicRouteInfo(topic string) {
	// Todo process lock timeout
	lockNamesrv.Lock()
	defer lockNamesrv.Unlock()

	RouteData, err := queryTopicRouteInfoFromServer(topic, requestTimeout)
	if err != nil {
		log.Warningf("query topic route from server error: %s", err)
		return
	}

	if RouteData == nil {
		log.Warningf("queryTopicRouteInfoFromServer return nil, Topic: %s", topic)
		return
	}

	var changed bool
	oldRouteData, exist := routeDataMap.Load(topic)
	if !exist || RouteData == nil {
		changed = true
	} else {
		changed = topicRouteDataIsChange(oldRouteData.(*topicRouteData), RouteData)
	}

	if !changed {
		changed = isNeedUpdateTopicRouteInfo(topic)
	} else {
		log.Infof("the topic[%s] route info changed, old[%s] ,new[%s]", topic, oldRouteData, RouteData)
	}

	if !changed {
		return
	}

	newTopicRouteData := RouteData.clone()

	for _, brokerData := range newTopicRouteData.brokerDataList {
		brokerAddressesMap.Store(brokerData.brokerName, brokerData.brokerAddresses)
	}

	// update publish info
	publishInfo := RouteData2PublishInfo(topic, RouteData)
	publishInfo.HaveTopicRouterInfo = true

	old, _ := publishInfoMap.Load(topic)
	publishInfoMap.Store(topic, publishInfoMap)
	if old != nil {
		log.Infof("Old TopicPublishInfo [%s] removed.", old)
	}
}

func FindBrokerAddressInPublish(brokerName string) string {
	bd, exist := brokerAddressesMap.Load(brokerName)

	if !exist {
		return ""
	}

	return bd.(*BrokerData).brokerAddresses[MasterId]
}

func FindBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) *FindBrokerResult {
	var (
		brokerAddr = ""
		slave      = false
		found      = false
	)

	bd, exist := brokerAddressesMap.Load(brokerName)

	if exist {
		for k, v := range bd.(*BrokerData).brokerAddresses {
			if v != "" {
				found = true
				if k != MasterId {
					slave = true
				}
				break
			}
		}
	}

	var result *FindBrokerResult
	if found {
		result = &FindBrokerResult{
			BrokerAddr:    brokerName,
			Slave:         slave,
			BrokerVersion: findBrokerVersion(brokerName, brokerAddr),
		}
	}

	return result
}

func FetchSubscribeMessageQueues(topic string) ([]*MessageQueue, error) {
	routeData, err := queryTopicRouteInfoFromServer(topic, 3*time.Second)

	if err != nil {
		return nil, err
	}

	mqs := make([]*MessageQueue, 0)

	for _, qd := range routeData.queueDataList {
		if queueIsReadable(qd.perm) {
			for i := 0; i < qd.readQueueNums; i++ {
				mqs = append(mqs, &MessageQueue{Topic: topic, BrokerName: qd.brokerName, QueueId: i})
			}
		}
	}

	return mqs, nil
}

func findBrokerVersion(brokerName, brokerAddr string) int {
	versions, exist := brokerVersionMap.Load(brokerName)

	if !exist {
		return 0
	}

	v, exist := versions.(map[string]int)[brokerAddr]

	if exist {
		return v
	}
	return 0
}

func queryTopicRouteInfoFromServer(topic string, timeout time.Duration) (*topicRouteData, error) {
	request := &GetRouteInfoRequest{
		Topic: topic,
	}
	rc := remote.NewRemotingCommand(GetRouteInfoByTopic, request, nil)

	response, err := remote.InvokeSync(getNameServerAddress(), rc, timeout)

	if err != nil {
		return nil, err
	}

	switch response.Code {
	case Success:
		if response.Body == nil {
			return nil, errors.New(response.Remark)
		}
		RouteData := &topicRouteData{}
		err = json.Unmarshal(response.Body, RouteData)
		if err != nil {
			log.Warningf("unmarshal topicRouteData error: %s", err)
			return nil, err
		}
		return RouteData, nil
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

	return !exist || value.(*TopicPublishInfo).isOK()
}

func RouteData2PublishInfo(topic string, data *topicRouteData) *TopicPublishInfo {
	publishInfo := &TopicPublishInfo{
		RouteData:  data,
		OrderTopic: false,
	}

	if data.OrderTopicConf != "" {
		brokers := strings.Split(data.OrderTopicConf, ";")
		for _, broker := range brokers {
			item := strings.Split(broker, ":")
			nums, _ := strconv.Atoi(item[1])
			for i := 0; i < nums; i++ {
				mq := &MessageQueue{
					Topic:      topic,
					BrokerName: item[0],
					QueueId:    i,
				}
				publishInfo.MqList = append(publishInfo.MqList, mq)
			}
		}

		publishInfo.OrderTopic = true
		return publishInfo
	}

	qds := data.queueDataList
	sort.Slice(qds, func(i, j int) bool {
		return i-j >= 0
	})

	for _, qd := range qds {
		if !queueIsWriteable(qd.perm) {
			continue
		}

		var bData *BrokerData
		for _, bd := range data.brokerDataList {
			if bd.brokerName == qd.brokerName {
				bData = bd
				break
			}
		}

		if bData == nil || bData.brokerAddresses[MasterId] == "" {
			continue
		}

		for i := 0; i < qd.writeQueueNums; i++ {
			mq := &MessageQueue{
				Topic:      topic,
				BrokerName: qd.brokerName,
				QueueId:    i,
			}
			publishInfo.MqList = append(publishInfo.MqList, mq)
		}
	}

	return publishInfo
}

func getNameServerAddress() string {
	return ""
}

// topicRouteData topicRouteData
type topicRouteData struct {
	OrderTopicConf string
	queueDataList  []*QueueData
	brokerDataList []*BrokerData
}

func (RouteData *topicRouteData) clone() *topicRouteData {
	cloned := &topicRouteData{
		OrderTopicConf: RouteData.OrderTopicConf,
		queueDataList:  make([]*QueueData, len(RouteData.queueDataList)),
		brokerDataList: make([]*BrokerData, len(RouteData.brokerDataList)),
	}

	for index, value := range RouteData.queueDataList {
		cloned.queueDataList[index] = value
	}

	for index, value := range RouteData.brokerDataList {
		cloned.brokerDataList[index] = value
	}

	return cloned
}

func (RouteData *topicRouteData) equals(data *topicRouteData) bool {
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
