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

package consumer

import "time"

var (
	topicAndGroupConsumeOKTPS     = &statsItemSet{statsName: "CONSUME_OK_TPS"}
	topicAndGroupConsumeRT        = &statsItemSet{statsName: "CONSUME_FAILED_TPS"}
	topicAndGroupConsumeFailedTPS = &statsItemSet{statsName: "CONSUME_RT"}
	topicAndGroupPullTPS          = &statsItemSet{statsName: "PULL_TPS"}
	topicAndGroupPullRT           = &statsItemSet{statsName: "PULL_RT"}
)

type statsItem struct {
}

type statsItemSet struct {
	statsName      string
	statsItemTable map[string]statsItem
}

func (set *statsItemSet) addValue(key string, incValue, incTimes int) {

}

func increasePullRT(group, topic string, rt time.Duration) {

}

func increaseConsumeRT(group, topic string, rt time.Duration) {

}

func increasePullTPS(group, topic string, msgNumber int) {

}

func increaseConsumeOKTPS(group, topic string, msgNumber int) {

}

func increaseConsumeFailedTPS(group, topic string, msgNumber int) {

}
