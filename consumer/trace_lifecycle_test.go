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

package consumer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	atomic2 "go.uber.org/atomic"
)

type unavailableTrace struct{}

func (*unavailableTrace) Start()                            { panic("nil dispatcher started") }
func (*unavailableTrace) Close()                            { panic("nil dispatcher closed") }
func (*unavailableTrace) Append(internal.TraceContext) bool { panic("nil dispatcher invoked") }
func (*unavailableTrace) GetTraceTopicName() string         { panic("nil dispatcher invoked") }

func TestUnavailableTraceInvokesBusinessCallback(t *testing.T) {
	var typedNil *unavailableTrace
	for _, dispatcher := range []internal.TraceDispatcher{nil, typedNil} {
		interceptor := newTraceInterceptor(dispatcher)
		for _, businessError := range []error{nil, errors.New("business error")} {
			calls := 0
			ctx := context.WithValue(context.Background(), "test-key", "test-value")
			ctx = primitive.WithConsumerCtx(ctx, &primitive.ConsumeMessageContext{
				ConsumerGroup: "test-group",
				Msgs:          []*primitive.MessageExt{{Message: *primitive.NewMessage("topic", []byte("payload"))}},
				Properties:    map[string]string{},
			})
			request, reply := new(int), new(int)
			err := interceptor(ctx, request, reply, func(actual context.Context, req, resp interface{}) error {
				calls++
				require.Equal(t, ctx, actual)
				require.Same(t, request, req)
				require.Same(t, reply, resp)
				*resp.(*int) = 42
				return businessError
			})
			require.Equal(t, 1, calls)
			require.Equal(t, 42, *reply)
			require.Equal(t, businessError, err)
		}
	}
}

func TestFailedTraceInstallationIsOptional(t *testing.T) {
	options := defaultPushConsumerOptions()
	originalCount := len(options.Interceptors)
	WithTrace(&primitive.TraceConfig{})(&options)
	require.Nil(t, options.TraceDispatcher)
	require.Len(t, options.Interceptors, originalCount)
	WithSharedTrace(&primitive.TraceConfig{}, primitive.SharedTraceClientConfig{Key: "missing-factory"})(&options)
	require.Nil(t, options.TraceDispatcher)
	require.Len(t, options.Interceptors, originalCount)
}

func TestTraceReleasedOnConstructionFailure(t *testing.T) {
	var created, closed int32
	shared := primitive.SharedTraceClientConfig{Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
		atomic.AddInt32(&created, 1)
		return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), func() { atomic.AddInt32(&closed, 1) }, nil
	}}
	for i := 0; i < 3; i++ {
		_, err := NewPushConsumer(
			WithSharedTrace(&primitive.TraceConfig{}, shared),
			WithNsResolver(primitive.NewPassthroughResolver([]string{"invalid"})),
		)
		require.Error(t, err)
	}
	require.Equal(t, int32(3), atomic.LoadInt32(&created))
	require.Equal(t, atomic.LoadInt32(&created), atomic.LoadInt32(&closed))
}

func TestPullTraceReleasedOnConstructionFailure(t *testing.T) {
	var created, closed int32
	shared := primitive.SharedTraceClientConfig{Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
		atomic.AddInt32(&created, 1)
		return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), func() { atomic.AddInt32(&closed, 1) }, nil
	}}
	for i := 0; i < 3; i++ {
		_, err := NewPullConsumer(
			WithSharedTrace(&primitive.TraceConfig{}, shared),
			WithNsResolver(primitive.NewPassthroughResolver([]string{"invalid"})),
		)
		require.Error(t, err)
	}
	require.Equal(t, int32(3), atomic.LoadInt32(&created))
	require.Equal(t, atomic.LoadInt32(&created), atomic.LoadInt32(&closed))
}

func TestTraceReleasedOnStartFailure(t *testing.T) {
	for _, pull := range []bool{false, true} {
		var closed int32
		opts := defaultPushConsumerOptions()
		shared := primitive.SharedTraceClientConfig{Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
			return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), func() { atomic.AddInt32(&closed, 1) }, nil
		}}
		WithSharedTrace(&primitive.TraceConfig{}, shared)(&opts)
		// Invalid group fails validation before any business transport is started.
		dc := &defaultConsumer{consumerGroup: "", option: opts, state: atomic2.NewInt32(int32(internal.StateCreateJust))}
		var err error
		if pull {
			err = (&defaultPullConsumer{defaultConsumer: dc}).Start()
		} else {
			err = (&pushConsumer{defaultConsumer: dc}).Start()
		}
		require.Error(t, err)
		require.Equal(t, int32(1), atomic.LoadInt32(&closed))
	}
}

// An unsuccessful Start releases its client reference without unregistering a group.
func TestTraceShutdownBeforeStartOrAfterDuplicateGroup(t *testing.T) {
	for _, pull := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			ctrl := gomock.NewController(t)
			client := internal.NewMockRMQClient(ctrl)
			client.EXPECT().Shutdown()
			opts := defaultPushConsumerOptions()
			var closed int32
			WithSharedTrace(&primitive.TraceConfig{}, primitive.SharedTraceClientConfig{Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
				return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), func() { atomic.AddInt32(&closed, 1) }, nil
			}})(&opts)
			dc := &defaultConsumer{consumerGroup: "trace-test", option: opts, client: client, state: atomic2.NewInt32(int32(internal.StateCreateJust))}
			var c interface {
				Start() error
				Shutdown() error
			}
			if pull {
				c = &defaultPullConsumer{defaultConsumer: dc, done: make(chan struct{}), SubType: Assign}
			} else {
				c = &pushConsumer{defaultConsumer: dc, done: make(chan struct{})}
			}
			if failed {
				client.EXPECT().RegisterConsumer(gomock.Any(), gomock.Any()).Return(errors.New("duplicate group"))
				require.Error(t, c.Start())
			}
			require.NoError(t, c.Shutdown())
			require.NoError(t, c.Shutdown())
			require.Error(t, c.Start())
			require.Equal(t, int32(1), atomic.LoadInt32(&closed))
			ctrl.Finish()
		}
	}
}
