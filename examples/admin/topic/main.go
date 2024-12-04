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

package main

import (
	"context"
	"fmt"

	"github.com/apache/rocketmq-client-go/v2/admin"
	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func main() {
	topic := "newOne"
	// clusterName := "DefaultCluster"
	nameSrvAddr := []string{"127.0.0.1:9876"}
	brokerAddr := "127.0.0.1:10911"

	testAdmin, err := admin.NewAdmin(
		admin.WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)),
		admin.WithCredentials(primitive.Credentials{
			AccessKey: "RocketMQ",
			SecretKey: "12345678",
		}),
	)

	// topic list
	result, err := testAdmin.FetchAllTopicList(context.Background())
	if err != nil {
		fmt.Println("FetchAllTopicList error:", err.Error())
	}
	fmt.Println(result.TopicList)

	// get broker cluster info
	clusterInfo, err := testAdmin.GetBrokerClusterInfo(
		context.Background(),
	)
	fmt.Printf("Broker Cluster Info:\n%v\n", clusterInfo)
	if err != nil {
		fmt.Println("GetBrokerClusterInfo error:", err.Error())
	}

	// create topic
	err = testAdmin.CreateTopic(
		context.Background(),
		admin.WithTopicCreate(topic),
		// admin will resolve broker name to broker address
		admin.WithBrokerNameCreate("broker-abc"),
	)
	if err != nil {
		fmt.Println("Create topic error:", err.Error())
	}

	// try to create same-name topic again
	err = testAdmin.CreateTopic(
		context.Background(),
		admin.WithTopicCreate(topic),
		admin.WithBrokerAddrCreate(brokerAddr),
		admin.WithOptNotOverrideCreate(true),
	)
	// it should raise `topic is exist` error if we have set the option `OptNotOverride`
	if err != nil {
		fmt.Println("Create topic error:", err.Error())
	}

	// delete topic
	err = testAdmin.DeleteTopic(
		context.Background(),
		admin.WithTopicDelete(topic),
		// admin.WithBrokerAddrDelete(brokerAddr),
		// admin.WithNameSrvAddr(nameSrvAddr),
	)
	if err != nil {
		fmt.Println("Delete topic error:", err.Error())
	}

	err = testAdmin.Close()
	if err != nil {
		fmt.Printf("Shutdown admin error: %s", err.Error())
	}
}
