package internal

import (
	"github.com/apache/rocketmq-client-go/options"
	"github.com/apache/rocketmq-client-go/primitive"
	"time"
)

type ConsumerOptions struct {
	options.ClientOptions
	NameServerAddrs []string

	/**
	 * Backtracking consumption time with second precision. Time format is
	 * 20131223171201<br>
	 * Implying Seventeen twelve and 01 seconds on December 23, 2013 year<br>
	 * Default backtracking consumption time Half an hour ago.
	 */
	ConsumeTimestamp string

	// The socket timeout in milliseconds
	ConsumerPullTimeout time.Duration

	// Concurrently max span offset.it has no effect on sequential consumption
	ConsumeConcurrentlyMaxSpan int

	// Flow control threshold on queue level, each message queue will cache at most 1000 messages by default,
	// Consider the {PullBatchSize}, the instantaneous value may exceed the limit
	PullThresholdForQueue int64

	// Limit the cached message size on queue level, each message queue will cache at most 100 MiB messages by default,
	// Consider the {@code pullBatchSize}, the instantaneous value may exceed the limit
	//
	// The size of a message only measured by message body, so it's not accurate
	PullThresholdSizeForQueue int

	// Flow control threshold on topic level, default value is -1(Unlimited)
	//
	// The value of {@code pullThresholdForQueue} will be overwrote and calculated based on
	// {@code pullThresholdForTopic} if it is't unlimited
	//
	// For example, if the value of pullThresholdForTopic is 1000 and 10 message queues are assigned to this consumer,
	// then pullThresholdForQueue will be set to 100
	PullThresholdForTopic int

	// Limit the cached message size on topic level, default value is -1 MiB(Unlimited)
	//
	// The value of {@code pullThresholdSizeForQueue} will be overwrote and calculated based on
	// {@code pullThresholdSizeForTopic} if it is't unlimited
	//
	// For example, if the value of pullThresholdSizeForTopic is 1000 MiB and 10 message queues are
	// assigned to this consumer, then pullThresholdSizeForQueue will be set to 100 MiB
	PullThresholdSizeForTopic int

	// Message pull Interval
	PullInterval time.Duration

	// Batch consumption size
	ConsumeMessageBatchMaxSize int

	// Batch pull size
	PullBatchSize int32

	// Whether update subscription relationship when every pull
	PostSubscriptionWhenPull bool

	// Max re-consume times. -1 means 16 times.
	//
	// If messages are re-consumed more than {@link #maxReconsumeTimes} before Success, it's be directed to a deletion
	// queue waiting.
	MaxReconsumeTimes int

	// Suspending pulling time for cases requiring slow pulling like flow-control scenario.
	SuspendCurrentQueueTimeMillis time.Duration

	// Maximum amount of time a message may block the consuming thread.
	ConsumeTimeout time.Duration

	ConsumerModel  primitive.MessageModel
	Strategy       primitive.AllocateStrategy
	ConsumeOrderly bool
	FromWhere      primitive.ConsumeFromWhere
	// TODO traceDispatcher

	Interceptors []primitive.Interceptor
}
