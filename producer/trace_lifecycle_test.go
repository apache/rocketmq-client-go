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

package producer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
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
			ctx = primitive.WithProducerCtx(ctx, &primitive.ProducerCtx{
				ProducerGroup: "test-group",
				Message:       *primitive.NewMessage("topic", []byte("payload")),
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
	options := defaultProducerOptions()
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
		_, err := NewDefaultProducer(
			WithSharedTrace(&primitive.TraceConfig{}, shared),
			WithNsResolver(primitive.NewPassthroughResolver([]string{"invalid"})),
		)
		require.Error(t, err)
	}
	require.Equal(t, int32(3), atomic.LoadInt32(&created))
	require.Equal(t, atomic.LoadInt32(&created), atomic.LoadInt32(&closed))
}

func TestTraceReleasedOnStartFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var closed int32
	opts := defaultProducerOptions()
	shared := primitive.SharedTraceClientConfig{Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
		return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), func() { atomic.AddInt32(&closed, 1) }, nil
	}}
	WithSharedTrace(&primitive.TraceConfig{}, shared)(&opts)
	client := internal.NewMockRMQClient(ctrl)
	client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).Return(errors.New("duplicate group"))
	p := &defaultProducer{client: client, options: opts}
	require.Error(t, p.Start())
	require.Equal(t, int32(1), atomic.LoadInt32(&closed))
}

func TestTraceShutdownBeforeStartOrAfterDuplicateGroup(t *testing.T) {
	for _, failed := range []bool{false, true} {
		ctrl := gomock.NewController(t)
		client := internal.NewMockRMQClient(ctrl)
		client.EXPECT().Shutdown()
		opts := defaultProducerOptions()
		var closed int32
		WithSharedTrace(&primitive.TraceConfig{}, primitive.SharedTraceClientConfig{Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
			return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), func() { atomic.AddInt32(&closed, 1) }, nil
		}})(&opts)
		p := &defaultProducer{client: client, options: opts}
		if failed {
			client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).Return(errors.New("duplicate group"))
			require.Error(t, p.Start())
		}
		require.NoError(t, p.Shutdown())
		require.NoError(t, p.Shutdown())
		require.Error(t, p.Start())
		require.Equal(t, int32(1), atomic.LoadInt32(&closed))
		ctrl.Finish()
	}
}

func TestTraceConcurrentStartShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := internal.NewMockRMQClient(ctrl)
	registered, release := make(chan struct{}), make(chan struct{})
	client.EXPECT().RegisterProducer(gomock.Any(), gomock.Any()).DoAndReturn(func(string, internal.InnerProducer) error { close(registered); <-release; return nil })
	client.EXPECT().Start()
	client.EXPECT().UnregisterProducer(gomock.Any())
	client.EXPECT().Shutdown()
	p := &defaultProducer{client: client, options: defaultProducerOptions()}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := p.Start(); err != nil {
			t.Error(err)
		}
	}()
	<-registered
	go func() {
		defer wg.Done()
		if err := p.Shutdown(); err != nil {
			t.Error(err)
		}
	}()
	close(release)
	wg.Wait()
	require.NoError(t, p.Shutdown())
	require.Error(t, p.Start())
}
