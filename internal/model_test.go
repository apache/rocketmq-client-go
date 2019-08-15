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
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/apache/rocketmq-client-go/internal/utils"
)

func TestHeartbeatData(t *testing.T) {
	Convey("test heatbeat json", t, func() {

		Convey("producerData set marshal", func() {
			pData := &producerData{
				GroupName: "group name",
			}
			pData2 := &producerData{
				GroupName: "group name 2",
			}
			set := utils.NewSet()
			set.Add(pData)
			set.Add(pData2)

			v, err := json.Marshal(set)
			assert.Nil(t, err)
			fmt.Printf("json producer set: %s", string(v))
		})

		Convey("producer heatbeat", func() {

			hbt := NewHeartbeatData("producer client id")
			p1 := &producerData{
				GroupName: "group name",
			}
			p2 := &producerData{
				GroupName: "group name 2",
			}

			hbt.ProducerDatas.Add(p1)
			hbt.ProducerDatas.Add(p2)

			v, err := json.Marshal(hbt)
			//ShouldBeNil(t, err)
			assert.Nil(t, err)
			fmt.Printf("json producer: %s\n", string(v))
		})

		Convey("consumer heartbeat", func() {

			hbt := NewHeartbeatData("consumer client id")
			c1 := consumerData{
				GroupName: "consumer data 1",
			}
			c2 := consumerData{
				GroupName: "consumer data 2",
			}
			hbt.ConsumerDatas.Add(c1)
			hbt.ConsumerDatas.Add(c2)

			v, err := json.Marshal(hbt)
			//ShouldBeNil(t, err)
			assert.Nil(t, err)
			fmt.Printf("json consumer: %s\n", string(v))
		})

		Convey("producer & consumer heartbeat", func() {

			hbt := NewHeartbeatData("consumer client id")

			p1 := &producerData{
				GroupName: "group name",
			}
			p2 := &producerData{
				GroupName: "group name 2",
			}

			hbt.ProducerDatas.Add(p1)
			hbt.ProducerDatas.Add(p2)

			c1 := consumerData{
				GroupName: "consumer data 1",
			}
			c2 := consumerData{
				GroupName: "consumer data 2",
			}
			hbt.ConsumerDatas.Add(c1)
			hbt.ConsumerDatas.Add(c2)

			v, err := json.Marshal(hbt)
			//ShouldBeNil(t, err)
			assert.Nil(t, err)
			fmt.Printf("json producer & consumer: %s\n", string(v))
		})
	})

}
