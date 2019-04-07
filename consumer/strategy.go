package consumer

import "github.com/apache/rocketmq-client-go/kernel"

type AllocateMessageQueueStrategy interface {
	allocate(string, string, []*kernel.MessageQueue, []string) []*kernel.MessageQueue
	name() string
}
