package rocketmq

type Message struct {
	MessageId        string
	Topic            string
	Body             []byte
	Tag              string
	Keys             []string
	UserProperties   map[string]string
	SystemProperties map[string]string
	MaxAttemptTimes  int
}
