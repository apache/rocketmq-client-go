package rocketmq

type Topic struct {
	name              string
	resourceNamespace string
}

func (t Topic) GetName() string {
	return t.name
}
