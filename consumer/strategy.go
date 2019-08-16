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

import (
	"github.com/apache/rocketmq-client-go/internal/utils"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/rlog"
	"strings"
)

// Strategy Algorithm for message allocating between consumers
// An allocate strategy proxy for based on machine room nearside priority. An actual allocate strategy can be
// specified.
//
// If any consumer is alive in a machine room, the message queue of the broker which is deployed in the same machine
// should only be allocated to those. Otherwise, those message queues can be shared along all consumers since there are
// no alive consumer to monopolize them.
//
// Average Hashing queue algorithm
// Cycle average Hashing queue algorithm
// Use Message QueueID specified
// Computer room Hashing queue algorithm, such as Alipay logic room
// Consistent Hashing queue algorithm

type AllocateStrategy func(string, string, []*primitive.MessageQueue, []string) []*primitive.MessageQueue

func AllocateByAveragely(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	if currentCID == "" || len(mqAll) == 0 || len(cidAll) == 0 {
		return nil
	}

	var (
		find  bool
		index int
	)
	for idx := range cidAll {
		if cidAll[idx] == currentCID {
			find = true
			index = idx
			break
		}
	}
	if !find {
		rlog.Warnf("[BUG] ConsumerGroup=%s, ConsumerId=%s not in cidAll:%+v", consumerGroup, currentCID, cidAll)
		return nil
	}

	mqSize := len(mqAll)
	cidSize := len(cidAll)
	mod := mqSize % cidSize

	var averageSize int
	if mqSize <= cidSize {
		averageSize = 1
	} else {
		if mod > 0 && index < mod {
			averageSize = mqSize/cidSize + 1
		} else {
			averageSize = mqSize / cidSize
		}
	}

	var startIndex int
	if mod > 0 && index < mod {
		startIndex = index * averageSize
	} else {
		startIndex = index*averageSize + mod
	}

	num := utils.MinInt(averageSize, mqSize-startIndex)
	result := []*primitive.MessageQueue{}
	for i := 0; i < num; i++ {
		result = append(result, mqAll[(startIndex+i)%mqSize])
	}
	return result
}

func AllocateByAveragelyCircle(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	if currentCID == "" || len(mqAll) == 0 || len(cidAll) == 0 {
		return nil
	}

	var (
		find  bool
		index int
	)
	for idx := range cidAll {
		if cidAll[idx] == currentCID {
			find = true
			index = idx
			break
		}
	}
	if !find {
		rlog.Warnf("[BUG] ConsumerGroup=%s, ConsumerId=%s not in cidAll:%+v", consumerGroup, currentCID, cidAll)
		return nil
	}

	result := []*primitive.MessageQueue{}
	for i := index; i < len(mqAll); i++ {
		if i%len(cidAll) == index {
			result = append(result, mqAll[i])
		}
	}
	return result
}

// TODO
func AllocateByMachineNearby(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	return AllocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func AllocateByConfig(list []*primitive.MessageQueue) AllocateStrategy {
	return func(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue, cidAll []string) []*primitive.MessageQueue {
		return list
	}
}

func AllocateByMachineRoom(consumeridcs []string) AllocateStrategy {
	return func(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue, cidAll []string) []*primitive.MessageQueue {
		if currentCID == "" || len(mqAll) == 0 || len(cidAll) == 0 {
			return nil
		}

		var (
			find  bool
			index int
		)
		for idx := range cidAll {
			if cidAll[idx] == currentCID {
				find = true
				index = idx
				break
			}
		}
		if !find {
			rlog.Warnf("[BUG] ConsumerGroup=%s, ConsumerId=%s not in cidAll:%+v", consumerGroup, currentCID, cidAll)
			return nil
		}

		premqAll := []*primitive.MessageQueue{}
		for _, mq := range mqAll {
			temp := strings.Split(mq.BrokerName, "@")
			if len(temp) == 2 {
				for _, idc := range consumeridcs {
					if idc == temp[0] {
						premqAll = append(premqAll, mq)
					}
				}
			}
		}

		mod := len(premqAll) / len(cidAll)
		rem := len(premqAll) % len(cidAll)
		startIndex := mod * index
		endIndex := startIndex + mod

		result := []*primitive.MessageQueue{}
		for i := startIndex; i < endIndex; i++ {
			result = append(result, mqAll[i])
		}
		if rem > index {
			result = append(result, premqAll[index+mod*len(cidAll)])
		}
		return result
	}
}

func AllocateByConsistentHash(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	return AllocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}
