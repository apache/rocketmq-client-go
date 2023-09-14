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
	"encoding/json"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"github.com/pkg/errors"
	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v2"
	"strings"
)

type ClusterInfo struct {
	BrokerAddrTable  map[string]BrokerData `json:"brokerAddrTable"`
	ClusterAddrTable map[string][]string   `json:"clusterAddrTable"`
}

type ClusterInfoMap struct {
	BrokerAddrTable  map[string]string `json:"brokerAddrTable"`
	ClusterAddrTable map[string]string `json:"clusterAddrTable"`
}

func ParseClusterInfo(jsonString string) (cluster *ClusterInfo, errResult error) {
	var brokerAddrTableResult = gjson.Get(jsonString, "brokerAddrTable")
	if !brokerAddrTableResult.Exists() {
		return nil, errors.New("json key for brokerAddrTable not exist")
	}
	var clusterAddrTableResult = gjson.Get(jsonString, "clusterAddrTable")
	if !clusterAddrTableResult.Exists() {
		return nil, errors.New("json key for clusterAddrTable not exist")
	}

	var mapAndArray, err = ParseClusterAddrTable(clusterAddrTableResult.String())
	if err != nil {
		return nil, err
	}

	if len(mapAndArray) <= 0 {
		return nil, nil
	}

	var clusterInfo = &ClusterInfo{}
	clusterInfo.ClusterAddrTable, errResult = ParseClusterAddrTable(clusterAddrTableResult.String())
	if errResult != nil {
		return nil, errResult
	}
	clusterInfo.BrokerAddrTable, errResult = ParseBrokerAddrTable(brokerAddrTableResult.String(), clusterInfo.ClusterAddrTable)
	if errResult != nil {
		return nil, errResult
	}
	return clusterInfo, nil
}

// parse broker addr table
// input like : {"broker-a":{"brokerAddrs":{0:"127.0.0.1:10911"},"brokerName":"broker-a","cluster":"DefaultCluster"}}
// result.key = broker name, result.value=BrokerData
func ParseBrokerAddrTable(jsonString string, clusterAndBrokerNamesMap map[string][]string) (map[string]BrokerData, error) {
	var brokerMap = make(map[string]BrokerData)
	for _, brokerNames := range clusterAndBrokerNamesMap {
		for _, brokerName := range brokerNames {
			var brokerInfo = gjson.Get(jsonString, brokerName).String()
			var brokerAddrsString = gjson.Get(brokerInfo, "brokerAddrs").String() // {0:"x.x.x.x:10911"}
			var brokerName1 = gjson.Get(brokerInfo, "brokerName").String()
			var cluster = gjson.Get(brokerInfo, "cluster").String()

			bd := BrokerData{
				Cluster:         cluster,
				BrokerName:      brokerName1,
				BrokerAddresses: ParseBrokerAddrs(brokerAddrsString),
			}

			brokerMap[brokerName1] = bd
		}
	}

	return brokerMap, nil
}

// input like : "{0:"127.0.0.1:10911"}" to map directly,can't parse, so get one by one
// result.key = broker id , result.value = broker address and port
func ParseBrokerAddrs1(jsonString string) map[int64]string {
	var resultMap = make(map[int64]string)
	var broker0Addr = gjson.Get(jsonString, "0")
	if broker0Addr.Exists() {
		resultMap[0] = broker0Addr.String()
	}
	var broker1Addr = gjson.Get(jsonString, "1")
	if broker1Addr.Exists() {
		resultMap[1] = broker1Addr.String()
	}
	return resultMap
}

// input like : "{0:"127.0.0.1:10911"}" to map directly,can't parse, so get one by one
// result.key = broker id , result.value = broker address and port
func ParseBrokerAddrs(jsonString string) map[int64]string {
	// for yaml parse, do replace
	jsonString = strings.ReplaceAll(jsonString, "0:", "0: ")
	jsonString = strings.ReplaceAll(jsonString, "1:", "1: ")

	var yamlmap map[int64]string
	err := yaml.Unmarshal([]byte(jsonString), &yamlmap)
	if err != nil {
		rlog.Error("parse addr error "+jsonString, nil)
		return nil
	}

	return yamlmap
}

// input like : {"DefaultCluster":["broker-a"]}
// result.key = cluster name, result.value = broker names
func ParseClusterAddrTable(jsonString string) (map[string][]string, error) {
	var result = make(map[string][]string)
	err := json.Unmarshal([]byte(jsonString), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
