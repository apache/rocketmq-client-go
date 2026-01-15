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
	"regexp"

	"github.com/tidwall/gjson"
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
	TopicList  []string
	BrokerAddr string
	RemotingSerializable
}

type SubscriptionGroupWrapper struct {
	SubscriptionGroupTable map[string]SubscriptionGroupConfig
	DataVersion            DataVersion
	RemotingSerializable
}

type DataVersion struct {
	Timestamp int64
	Counter   int32
}

type SubscriptionGroupConfig struct {
	GroupName                      string
	ConsumeEnable                  bool
	ConsumeFromMinEnable           bool
	ConsumeBroadcastEnable         bool
	RetryMaxTimes                  int
	RetryQueueNums                 int
	BrokerId                       int
	WhichBrokerWhenConsumeSlowly   int
	NotifyConsumerIdsChangedEnable bool
}

type BrokerClusterInfo struct {
	BrokerAddrTable  map[string]ClusterBrokerData `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string          `json:"clusterAddrTable"`
}

type ClusterBrokerData struct {
	Cluster     string            `json:"cluster"`
	BrokerName  string            `json:"brokerName"`
	BrokerAddrs map[string]string `json:"brokerAddrs"`
}

// normalizeNumericObjectKeys {0:"ip"} -> {"0":"ip"}
func normalizeNumericObjectKeys(raw string) string {
	re := regexp.MustCompile(`([\{,]\s*)(\d+)(\s*:)`)
	return re.ReplaceAllString(raw, `$1"$2"$3`)
}

func (info *BrokerClusterInfo) Decode(data []byte, classOfT interface{}) (interface{}, error) {
	res := gjson.ParseBytes(data)

	info.BrokerAddrTable = make(map[string]ClusterBrokerData)
	info.ClusterAddrTable = make(map[string][]string)

	res.Get("brokerAddrTable").ForEach(func(k, v gjson.Result) bool {
		brokerName := k.String()
		raw := v.Get("brokerAddrs").Raw
		if raw == "" {
			raw = v.Get("brokerAddrs").String()
		}
		fixed := normalizeNumericObjectKeys(raw)
		addrs := make(map[string]string)
		_ = json.Unmarshal([]byte(fixed), &addrs)

		info.BrokerAddrTable[brokerName] = ClusterBrokerData{
			Cluster:     v.Get("cluster").String(),
			BrokerName:  v.Get("brokerName").String(),
			BrokerAddrs: addrs,
		}
		return true
	})

	res.Get("clusterAddrTable").ForEach(func(k, v gjson.Result) bool {
		cluster := k.String()
		list := make([]string, 0, len(v.Array()))
		v.ForEach(func(_, item gjson.Result) bool {
			list = append(list, item.String())
			return true
		})
		info.ClusterAddrTable[cluster] = list
		return true
	})

	return info, nil
}
