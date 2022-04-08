package rocketmq

type INameServerResolver interface {
	ResolveAsync() []string
}
