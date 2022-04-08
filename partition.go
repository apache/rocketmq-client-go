package rocketmq

import v1 "github.com/apache/rocketmq-client-go/v2/Protos/apache/rocketmq/v1"

type Partition struct {
	topic      *Topic
	broker     *Broker
	id         int32
	permission v1.Permission
}

func (p Partition) GetTopic() *Topic {
	return p.topic
}

func (p Partition) GetBroker() *Broker {
	return p.broker
}

func (p Partition) GetId() int32 {
	return p.id
}

func (p Partition) GetPermission() v1.Permission {
	return p.permission
}
