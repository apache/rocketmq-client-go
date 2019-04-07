package consumer

import "github.com/apache/rocketmq-client-go/kernel"

type readType int

const (
	_ReadFromMemory readType = iota
	_ReadFromStore
	_ReadMemoryThenStore
)

type OffsetStore interface {
	load()
	persist(mqs []*kernel.MessageQueue)
	remove(mq *kernel.MessageQueue)
	read(mq *kernel.MessageQueue, t readType) int64
	update(mq *kernel.MessageQueue, offset int64, increaseOnly bool)
}

type LocalFileOffsetStore struct {
}

type RemoteBrokerOffsetStore struct {
}
