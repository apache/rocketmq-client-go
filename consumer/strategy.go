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
	"github.com/apache/rocketmq-client-go/kernel"
	"github.com/apache/rocketmq-client-go/rlog"
	"github.com/apache/rocketmq-client-go/utils"
)

// Strategy Algorithm for message allocating between consumers
type AllocateStrategy string

const (
	// An allocate strategy proxy for based on machine room nearside priority. An actual allocate strategy can be
	// specified.
	//
	// If any consumer is alive in a machine room, the message queue of the broker which is deployed in the same machine
	// should only be allocated to those. Otherwise, those message queues can be shared along all consumers since there are
	// no alive consumer to monopolize them.
	StrategyMachineNearby   = AllocateStrategy("MachineNearby")

	// Average Hashing queue algorithm
	StrategyAveragely       = AllocateStrategy("Averagely")

	// Cycle average Hashing queue algorithm
	StrategyAveragelyCircle = AllocateStrategy("AveragelyCircle")

	// Use Message Queue specified
	StrategyConfig          = AllocateStrategy("Config")

	// Computer room Hashing queue algorithm, such as Alipay logic room
	StrategyMachineRoom     = AllocateStrategy("MachineRoom")

	// Consistent Hashing queue algorithm
	StrategyConsistentHash  = AllocateStrategy("ConsistentHash")
)

func allocateByAveragely(consumerGroup, currentCID string, mqAll []*kernel.MessageQueue,
	cidAll []string) []*kernel.MessageQueue {
	if currentCID == "" || utils.IsArrayEmpty(mqAll) || utils.IsArrayEmpty(cidAll) {
		return nil
	}
	var (
		find bool
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
			averageSize = mqSize / cidSize +1
		} else {
			averageSize = mqSize / cidSize
		}
	}

	var startIndex int
	if mod > 0 && index < mod {
		startIndex = index * averageSize
	} else {
		startIndex = index * averageSize + mod
	}

	num := utils.MinInt(averageSize, mqSize - startIndex)
	result := make([]*kernel.MessageQueue, num)
	for i := 0; i < num; i++ {
		result[i] = mqAll[(startIndex + i) % mqSize]
	}
	return nil
}

// TODO
func allocateByMachineNearby(consumerGroup, currentCID string, mqAll []*kernel.MessageQueue,
	cidAll []string) []*kernel.MessageQueue {
	return allocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func allocateByAveragelyCircle(consumerGroup, currentCID string, mqAll []*kernel.MessageQueue,
	cidAll []string) []*kernel.MessageQueue {
	return allocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func allocateByConfig(consumerGroup, currentCID string, mqAll []*kernel.MessageQueue,
	cidAll []string) []*kernel.MessageQueue {
	return allocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func allocateByMachineRoom(consumerGroup, currentCID string, mqAll []*kernel.MessageQueue,
	cidAll []string) []*kernel.MessageQueue {
	return allocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}

func allocateByConsistentHash(consumerGroup, currentCID string, mqAll []*kernel.MessageQueue,
	cidAll []string) []*kernel.MessageQueue {
	return allocateByAveragely(consumerGroup, currentCID, mqAll, cidAll)
}
