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

package rocketmq

import (
	"context"
	"time"

	"github.com/bilinxing/rocketmq-client-go/v2/consumer"
	"github.com/bilinxing/rocketmq-client-go/v2/internal"
	"github.com/bilinxing/rocketmq-client-go/v2/primitive"
	"github.com/bilinxing/rocketmq-client-go/v2/producer"
)

type Producer interface {
	Start() error
	Shutdown() error
	SendSync(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error)
	SendAsync(ctx context.Context, mq func(ctx context.Context, result *primitive.SendResult, err error),
		msg ...*primitive.Message) error
	SendOneWay(ctx context.Context, mq ...*primitive.Message) error
	Request(ctx context.Context, ttl time.Duration, msg *primitive.Message) (*primitive.Message, error)
	RequestAsync(ctx context.Context, ttl time.Duration, callback internal.RequestCallback, msg *primitive.Message) error
}

func NewProducer(opts ...producer.Option) (Producer, error) {
	return producer.NewDefaultProducer(opts...)
}

type TransactionProducer interface {
	Start() error
	Shutdown() error
	SendMessageInTransaction(ctx context.Context, mq *primitive.Message) (*primitive.TransactionSendResult, error)
}

func NewTransactionProducer(listener primitive.TransactionListener, opts ...producer.Option) (TransactionProducer, error) {
	return producer.NewTransactionProducer(listener, opts...)
}

type PushConsumer interface {
	// Start the PushConsumer for consuming message
	Start() error

	// Shutdown the PushConsumer, all offset of MessageQueue will be sync to broker before process exit
	Shutdown() error
	// Subscribe a topic for consuming
	Subscribe(topic string, selector consumer.MessageSelector,
		f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error

	// Unsubscribe a topic
	Unsubscribe(topic string) error

	// Suspend the consumption
	Suspend()

	// Resume the consumption
	Resume()

	// GetOffsetDiffMap Get offset difference map
	GetOffsetDiffMap() map[string]int64
}

func NewPushConsumer(opts ...consumer.Option) (PushConsumer, error) {
	return consumer.NewPushConsumer(opts...)
}

type PullConsumer interface {
	// Start the PullConsumer for consuming message
	Start() error

	// Subscribe a topic for consuming
	Subscribe(topic string, selector consumer.MessageSelector) error

	// Unsubscribe a topic
	Unsubscribe(topic string) error

	// Shutdown the PullConsumer, all offset of MessageQueue will be commit to broker before process exit
	Shutdown() error

	// Poll messages with timeout.
	Poll(ctx context.Context, timeout time.Duration) (*consumer.ConsumeRequest, error)

	//ACK ACK
	ACK(ctx context.Context, cr *consumer.ConsumeRequest, consumeResult consumer.ConsumeResult)

	// Pull message of topic,  selector indicate which queue to pull.
	Pull(ctx context.Context, numbers int) (*primitive.PullResult, error)

	// PullFrom pull messages of queue from the offset to offset + numbers
	PullFrom(ctx context.Context, queue *primitive.MessageQueue, offset int64, numbers int) (*primitive.PullResult, error)

	// UpdateOffset updateOffset update offset of queue in mem
	UpdateOffset(queue *primitive.MessageQueue, offset int64) error

	// PersistOffset persist all offset in mem.
	PersistOffset(ctx context.Context, topic string) error

	// CurrentOffset return the current offset of queue in mem.
	CurrentOffset(queue *primitive.MessageQueue) (int64, error)
}

func NewPullConsumer(opts ...consumer.Option) (PullConsumer, error) {
	return consumer.NewPullConsumer(opts...)
}
