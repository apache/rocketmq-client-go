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
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/assert"
	"testing"
)

var nameSrvAddr = []string{"127.0.0.1:9876"}

func TestFetchAllTopicList(t *testing.T) {
	adminTool, err := NewAdmin(WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)))
	assert.True(t, err == nil)
	if err != nil {
		fmt.Println(err)
	}

	topicNames, error := adminTool.FetchAllTopicList(context.Background())
	assert.True(t, error == nil)
	assert.True(t, len(topicNames.TopicNameList) > 0)

	prettyJson, _ := json.MarshalIndent(topicNames, "", " ")
	fmt.Println(string(prettyJson))
}

func TestGetBrokerClusterInfo(t *testing.T) {
	adminTool, err := NewAdmin(WithResolver(primitive.NewPassthroughResolver(nameSrvAddr)))
	assert.True(t, err == nil)
	if err != nil {
		fmt.Println(err)
	}

	cluster, err := adminTool.GetBrokerClusterInfo(context.Background())
	assert.True(t, err == nil)
	assert.True(t, cluster != nil)

	prettyJson, _ := json.MarshalIndent(cluster, "", " ")
	fmt.Println(string(prettyJson))
}
