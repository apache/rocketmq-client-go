package common

import (
	"fmt"
	"sync"
)

var connMap sync.Map

// SendMessage with batch by sync
func SendMessage(topic string, msgs *[]Message) error {
	return nil
}

// SendMessageAsync send message with batch by async
func SendMessageAsync(topic string, msgs *[]Message, f func(result *SendResult)) error {
	return nil
}

// PullMessage with sync
func PullMessage(request *PullMessageRequest) error {
	return nil
}

// PullMessageAsync pull message async
func PullMessageAsync(request *PullMessageRequest, f func(result *PullResult)) error {
	return nil
}

// QueryMaxOffset with specific queueId and topic
func QueryMaxOffset(topic string, queueId int) error {
	return nil
}

// QueryConsumerOffset with specific queueId and topic of consumerGroup
func QueryConsumerOffset(consumerGroup, topic string, queue int) (int64, error) {
	return 0, nil
}

// SearchOffsetByTimestamp with specific queueId and topic
func SearchOffsetByTimestamp(topic string, queue int, timestamp int64) (int64, error) {
	return 0, nil
}

// UpdateConsumerOffset with specific queueId and topic
func UpdateConsumerOffset(consumerGroup, topic string, queue int, offset int64) error {
	return nil
}

//SendStatus message send result
type SendStatus int

const (
	SendOK SendStatus = iota
	SendFlushDiskTimeout
	SendFlushSlaveTimeout
	SendSlaveNotAvailable
)

// SendResult rocketmq send result
type SendResult struct {
	sendStatus    SendStatus
	msgID         string
	messageQueue  MessageQueue
	queueOffset   int64
	transactionID string
	offsetMsgID   string
	regionID      string
	traceOn       bool
}

// SendResult send message result to string(detail result)
func (result *SendResult) String() string {
	return fmt.Sprintf("SendResult [sendStatus=%d, msgId=%s, offsetMsgId=%s, queueOffset=%d, messageQueue=%s]",
		result.sendStatus, result.msgID, result.offsetMsgID, result.queueOffset, result.messageQueue.String())
}

// PullResult the pull result
type PullResult struct {
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	Status          PullStatus
	Messages        []*MessageExt
}

// PullStatus pull status
type PullStatus int

// predefined pull status
const (
	PullFound PullStatus = iota
	PullNoNewMsg
	PullNoMatchedMsg
	PullOffsetIllegal
	PullBrokerTimeout
)

// MessageQueue message queue
type MessageQueue struct {
	Topic      string `json:"topic"`
	BrokerName string `json:"brokerName"`
	QueueId    int32  `json:"queueId"`
}

func (mq *MessageQueue) String() string {
	return fmt.Sprintf("MessageQueue [topic=%s, brokerName=%s, queueId=%d]", mq.Topic, mq.BrokerName, mq.QueueId)
}
