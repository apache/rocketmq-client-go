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

	"github.com/apache/rocketmq-client-go/consumer"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/producer"
)

type Producer interface {
	Start() error
	Shutdown() error
	SendSync(context.Context, *primitive.Message) (*primitive.SendResult, error)
	SendAsync(context.Context, func(context.Context, *primitive.SendResult, error), *primitive.Message) error
	SendOneWay(context.Context, *primitive.Message) error
}

func NewProducer(opts ...producer.Option) (Producer, error) {
	return producer.NewDefaultProducer(opts...)
}

type PushConsumer interface {
	Start() error
	Shutdown() error
	Subscribe(topic string, selector consumer.MessageSelector,
		f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error
	Unsubscribe(string) error
}

func NewPushConsumer(opts ...consumer.Option) (PushConsumer, error) {
	return consumer.NewPushConsumer(opts...)
}

type PullConsumer interface {
	Start() error
	Shutdown() error
	Pull(context.Context, string, consumer.MessageSelector, int) (*primitive.PullResult, error)
	PullFrom(context.Context, *primitive.MessageQueue, int64, int) (*primitive.PullResult, error)
	CurrentOffset(*primitive.MessageQueue) (int64, error)
	UpdateOffset(*primitive.MessageQueue, int64) error
	PersistOffset(context.Context) error
}

func NewPullConsumer(opts ...consumer.Option) (PullConsumer, error) {
	return consumer.NewPullConsumer(opts...)
}
