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
	"context"
	"log"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/primitive"
)

func initAdmin(t *testing.T) Admin {
	var err error

	testAdmin, err := NewAdmin(WithResolver(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})))
	assert(err)
	return testAdmin
}

func TestCreateTopic(t *testing.T) {
	testAdmin := initAdmin(t)
	brokerAddr := "127.0.0.1:10911"

	err := testAdmin.CreateTopic(
		context.Background(),
		WithTopicCreate("newOne"),
		WithBrokerAddrCreate(brokerAddr),
	)
	assert(err)
	log.Printf("create topic to %v success", brokerAddr)
}

/*
//TODO: another implementation like sarama, without brokerAddr as input(would be in admin)
func TestCreateTopic(t *testing.T) {
	testAdmin := initAdmin(t)
	newTopic := "newOne"
	brokerAddr := "172.29.193.44:10911"

	err := testAdmin.CreateTopic(context.Background(), newTopic, brokerAddr)
	assert(err)
	log.Printf("create topic to %v success", brokerAddr)
}
*/

//TODO: another implementation like sarama, without clusterName/namSrvAddr as input(would be in admin)
func TestDeleteTopic(t *testing.T) {
	testAdmin := initAdmin(t)

	topic := "newOne"
	clusterName := "DefaultCluster"
	nameSrvAddr := "127.0.0.1:9876"
	brokerAddr := "127.0.0.1:10911"

	err := testAdmin.DeleteTopic(
		context.Background(),
		WithTopicDelete(topic),
		WithBrokerAddrDelete(brokerAddr),
		WithClusterName(clusterName),
		WithNameSrvAddr(nameSrvAddr),
	)
	assert(err)
	log.Printf("delete topic [%v] from cluster [%v] success", topic, clusterName)
	log.Printf("delete topic [%v] from NameServer success", topic)
}

/*
func TestTopicList(t *testing.T) {
	testAdmin := initAdmin(t)

	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: brokerName,
		QueueId:    0,
	}

	list := testAdmin.TopicList(context.Background(), mq)
	//assert(err)
	log.Printf("Topic List: %v", list)
}

func TestGetBrokerClusterInfo(t *testing.T) {
	testAdmin := initAdmin(t)

	mq := &primitive.MessageQueue{
		Topic:      topic,
		BrokerName: brokerName,
		QueueId:    0,
	}

	list, err := testAdmin.GetBrokerClusterInfo(context.Background(), mq)
	assert(err)
	log.Printf("Broker Cluster Info: %#v", list)
}
*/

func assert(err error) {
	if err != nil {
		panic(err)
	}
}
