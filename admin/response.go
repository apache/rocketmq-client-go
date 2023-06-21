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

import "encoding/json"

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
