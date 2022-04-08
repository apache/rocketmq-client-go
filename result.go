package rocketmq

type SendStatus int

const (
	SendOK SendStatus = iota
	SendFlushDiskTimeout
	SendFlushSlaveTimeout
	SendSlaveNotAvailable
	SendUnknownError
)

// SendResult RocketMQ send result
type SendResult struct {
	Status SendStatus
	MsgID  string
}
