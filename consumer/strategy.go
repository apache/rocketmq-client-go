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
	if currentCID == "" || utils.IsArrayEmpty(mqAll) || utils.IsArrayEmpty(cidAll) {
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
		rlog.Infof("[BUG] ConsumerGroup=%s, ConsumerId=%s not in cidAll:%+v", consumerGroup, currentCID, cidAll)
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
	result := make([]*primitive.MessageQueue, num)
	for i := 0; i < num; i++ {
		result[i] = mqAll[(startIndex+i)%mqSize]
	}
	return result
}

// TODO
func AllocateByMachineNearby(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	return AllocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func AllocateByAveragelyCircle(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	return AllocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func AllocateByConfig(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	return AllocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func AllocateByMachineRoom(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	return AllocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func AllocateByConsistentHash(consumerGroup, currentCID string, mqAll []*primitive.MessageQueue,
	cidAll []string) []*primitive.MessageQueue {
	return AllocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}
