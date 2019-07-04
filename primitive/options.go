/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package primitive

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/apache/rocketmq-client-go/utils"
)

type ProducerOptions struct {
	Interceptors []PInterceptor

	ClientOption
	NameServerAddr           string
	GroupName                string
	RetryTimesWhenSendFailed int
	UnitMode                 bool
}

func DefaultProducerOptions() ProducerOptions {
	return ProducerOptions{
		RetryTimesWhenSendFailed:  2,
	}
}

// ProducerOption configures how we create the producer by set ProducerOptions value.
type ProducerOption struct {
	Apply func(*ProducerOptions)
}

func NewOption(f func(options *ProducerOptions)) *ProducerOption {
	return &ProducerOption{
		Apply: f,
	}
}

// WithInterceptor returns a ProducerOption that specifies the interceptor for producer.
func WithInterceptor(f PInterceptor) *ProducerOption {
	return NewOption(func(options *ProducerOptions) {
		options.Interceptors = append(options.Interceptors, f)
	})
}

// WithChainInterceptor returns a ProducerOption that specifies the chained interceptor for producer.
// The first interceptor will be the outer most, while the last interceptor will be the inner most wrapper
// around the real call.
func WithChainInterceptor(fs ...PInterceptor) *ProducerOption {
	return NewOption(func(options *ProducerOptions) {
		options.Interceptors = append(options.Interceptors, fs...)
	})
}

// WithRetry return a ProducerOption that specifies the retry times when send failed.
// TODO: use retryMiddleeware instead.
func WithRetry(retries int) *ProducerOption {
	return  NewOption(func(options *ProducerOptions) {
		options.RetryTimesWhenSendFailed = retries
	})
}

type ConsumerOptions struct {
	ClientOption
	NameServerAddr string

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

	ConsumerModel  MessageModel
	Strategy       AllocateStrategy
	ConsumeOrderly bool
	FromWhere      ConsumeFromWhere
	// TODO traceDispatcher

	Interceptors []CInterceptor
}

func DefaultPushConsumerOptions() ConsumerOptions{
	return ConsumerOptions{
		ClientOption: ClientOption{
			InstanceName: "DEFAULT",
			ClientIP: utils.LocalIP(),
		},
		Strategy: AllocateByAveragely,
	}
}

type ConsumerOption struct {
	Apply func(*ConsumerOptions)
}

func NewConsumerOption(f func(*ConsumerOptions)) *ConsumerOption {
	return &ConsumerOption{
		Apply: f,
	}
}

func WithConsumerModel(m MessageModel) *ConsumerOption {
	return NewConsumerOption(func(options *ConsumerOptions) {
		options.ConsumerModel = m
	})
}

func WithConsumeFromWhere(w ConsumeFromWhere) *ConsumerOption{
	return NewConsumerOption(func(options *ConsumerOptions) {
		options.FromWhere = w
	})
}

func (opt *ClientOption) ChangeInstanceNameToPID() {
	if opt.InstanceName == "DEFAULT" {
		opt.InstanceName = strconv.Itoa(os.Getegid())
	}
}

func (opt *ClientOption) String() string {
	return fmt.Sprintf("ClientOption [ClientIP=%s, InstanceName=%s, "+
		"UnitMode=%v, UnitName=%s, VIPChannelEnabled=%v, UseTLS=%v]", opt.ClientIP,
		opt.InstanceName, opt.UnitMode, opt.UnitName, opt.VIPChannelEnabled, opt.UseTLS)
}

type ClientOption struct {
	NameServerAddr    string
	ClientIP          string
	InstanceName      string
	UnitMode          bool
	UnitName          string
	VIPChannelEnabled bool
	UseTLS            bool
}
