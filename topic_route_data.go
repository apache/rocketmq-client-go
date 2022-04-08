package rocketmq

type TopicRouteData struct {
	partitions []*Partition
}

func NewTopicRouteData(partitions []*Partition) *TopicRouteData {
	t := &TopicRouteData{
		partitions: partitions,
	}
	return t
}

func (t *TopicRouteData) Partitions() []*Partition {
	return t.partitions
}
