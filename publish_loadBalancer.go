package rocketmq

import (
	v1 "github.com/apache/rocketmq-client-go/v2/Protos/apache/rocketmq/v1"
	"math"
	"math/rand"
)

type PublishLoadBalancer struct {
	partitions      []*Partition
	roundRobinIndex int
}

func NewPublishLoadBalancer(route TopicRouteData) PublishLoadBalancer {
	p_ := make([]*Partition, 0)
	for _, v := range route.Partitions() {
		if v.permission == v1.Permission_NONE {
			continue
		}
		if v.permission == v1.Permission_READ {
			continue
		}
		p_ = append(p_, v)
	}
	return PublishLoadBalancer{
		partitions:      p_,
		roundRobinIndex: rand.Intn(len(p_)),
	}
}

func (p PublishLoadBalancer) update(route TopicRouteData) {
	p_ := make([]*Partition, 0)
	for _, v := range route.Partitions() {
		if v.permission == v1.Permission_NONE {
			continue
		}
		if v.permission == v1.Permission_READ {
			continue
		}
		p_ = append(p_, v)
	}
	p.partitions = p_
}

/**
 * Accept a partition if its broker is different.
 */
func acceptPartition(existing []*Partition, partition *Partition) bool {
	if len(existing) == 0 {
		return true
	}
	for _, v := range existing {
		if CompareBroker(v.GetBroker(), partition.GetBroker()) {
			return false
		}
	}
	return true
}

func (p PublishLoadBalancer) selectPartitions(maxAttemptTimes int) []*Partition {
	result := make([]*Partition, 0)

	if len(p.partitions) == 0 {
		return result
	}

	p.roundRobinIndex += 1
	start := p.roundRobinIndex
	found := 0

	for i := 0; i < len(p.partitions); i++ {
		idx := ((start + i) & math.MaxInt32) % len(p.partitions)
		if acceptPartition(result, p.partitions[idx]) {
			result = append(result, p.partitions[idx])
			found += 1
			if found >= maxAttemptTimes {
				break
			}
		}
	}
	return result
}
