package kernel

const (
	RetryGroupTopicPrefix = "%RETRY%"
	DefaultConsumerGroup  = "DEFAULT_CONSUMER"
)

func GetRetryTopic(group string) string {
	return RetryGroupTopicPrefix + group
}
