package common

type SendMessageRequest struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int    `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int    `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int    `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int    `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
	MaxReconsumeTimes     int    `json:"maxReconsumeTimes"`
}

type PullMessageRequest struct {
	ConsumerGroup        string `json:"consumerGroup"`
	Topic                string `json:"topic"`
	QueueId              int32  `json:"queueId"`
	QueueOffset          int64  `json:"queueOffset"`
	MaxMsgNums           int32  `json:"maxMsgNums"`
	SysFlag              int32  `json:"sysFlag"`
	CommitOffset         int64  `json:"commitOffset"`
	SuspendTimeoutMillis int64  `json:"suspendTimeoutMillis"`
	Subscription         string `json:"subscription"`
	SubVersion           int64  `json:"subVersion"`
}

type GetMaxOffsetRequest struct {
	Topic   string `json:"topic"`
	QueueId int32  `json:"queueId"`
}

type QueryConsumerOffsetRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
}

type SearchOffsetRequest struct {
	Topic     string `json:"topic"`
	QueueId   int32  `json:"queueId"`
	Timestamp int64  `json:"timestamp"`
}

type UpdateConsumerOffsetRequest struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
	CommitOffset  int64  `json:"commitOffset"`
}
