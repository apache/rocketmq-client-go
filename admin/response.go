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

package admin

import (
	"encoding/json"
	"fmt"
	"github.com/robertkrimen/otto"
)

type RemotingSerializable struct {
}

func (r *RemotingSerializable) Encode(obj interface{}) ([]byte, error) {
	jsonStr := r.ToJson(obj, false)
	if jsonStr != "" {
		return []byte(jsonStr), nil
	}
	return nil, nil
}

func (r *RemotingSerializable) ToJson(obj interface{}, prettyFormat bool) string {
	if prettyFormat {
		jsonBytes, err := json.MarshalIndent(obj, "", "  ")
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	} else {
		jsonBytes, err := json.Marshal(obj)
		if err != nil {
			return ""
		}
		return string(jsonBytes)
	}
}

func (r *RemotingSerializable) Decode(data []byte, classOfT interface{}) (interface{}, error) {
	jsonStr := string(data)
	return r.FromJson(jsonStr, classOfT)
}

func (r *RemotingSerializable) FromJson(jsonStr string, classOfT interface{}) (interface{}, error) {
	err := json.Unmarshal([]byte(jsonStr), classOfT)
	if err != nil {
		return nil, err
	}
	return classOfT, nil
}

type TopicList struct {
	TopicList  []string `json:"topicList"`
	BrokerAddr string   `json:"brokerAddr"`
	RemotingSerializable
}

type SubscriptionGroupWrapper struct {
	SubscriptionGroupTable map[string]SubscriptionGroupConfig `json:"subscriptionGroupTable"`
	DataVersion            DataVersion                        `json:"dataVersion"`
	RemotingSerializable
}

type DataVersion struct {
	Timestamp int64 `json:"timestamp"`
	Counter   int32 `json:"counter"`
}

type SubscriptionGroupConfig struct {
	GroupName                      string `json:"groupName"`
	ConsumeEnable                  bool   `json:"consumeEnable"`
	ConsumeFromMinEnable           bool   `json:"consumeFromMinEnable"`
	ConsumeBroadcastEnable         bool   `json:"consumeBroadcastEnable"`
	RetryMaxTimes                  int    `json:"retryMaxTimes"`
	RetryQueueNums                 int    `json:"retryQueueNums"`
	BrokerId                       int    `json:"brokerId"`
	WhichBrokerWhenConsumeSlowly   int    `json:"whichBrokerWhenConsumeSlowly"`
	NotifyConsumerIdsChangedEnable bool   `json:"notifyConsumerIdsChangedEnable"`
}

type ClusterInfo struct {
	BrokerAddrTable  map[string]BrokerData `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string   `json:"clusterAddrTable"`
	RemotingSerializable
}

type BrokerData struct {
	Cluster     string           `json:"cluster"`
	BrokerName  string           `json:"brokerName"`
	BrokerAddrs map[int64]string `json:"brokerAddrs"`
}

func BuildClusterInformation(response []byte) string {
	vm := otto.New()
	vm.Set("source", string(response))
	value, err := vm.Run(`
	    var code = 'JSON.stringify(' + source + ')';
	   eval(code);
	`)
	if err != nil {
		fmt.Printf("Format reponse error: %s", err.Error())
	}
	result, _ := value.ToString()
	return result
}
