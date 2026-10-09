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
	"testing"

	"github.com/apache/rocketmq-client-go/v2/internal"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/require"
)

type replacementTraceDispatcher struct {
	starts, closes int
	records        []internal.TraceContext
}

func (d *replacementTraceDispatcher) Start()                  { d.starts++ }
func (d *replacementTraceDispatcher) Close()                  { d.closes++ }
func (*replacementTraceDispatcher) GetTraceTopicName() string { return "trace-topic" }
func (d *replacementTraceDispatcher) Append(ctx internal.TraceContext) bool {
	d.records = append(d.records, ctx)
	return d.closes == 0
}

func TestTraceReplacementPreservesInterceptors(t *testing.T) {
	options := defaultProducerOptions()
	var calls []string
	userInterceptor := func(name string) primitive.Interceptor {
		return func(ctx context.Context, req, reply interface{}, next primitive.Invoker) error {
			calls = append(calls, name)
			return next(ctx, req, reply)
		}
	}
	WithInterceptor(userInterceptor("first"))(&options)
	old := &replacementTraceDispatcher{}
	installTraceInterceptor(&options, old)
	WithInterceptor(userInterceptor("second"))(&options)
	middle, current := &replacementTraceDispatcher{}, &replacementTraceDispatcher{}
	installTraceInterceptor(&options, middle)
	installTraceInterceptor(&options, current)
	// An unavailable replacement must preserve the working dispatcher and chain.
	var typedNil *replacementTraceDispatcher
	for _, unavailable := range []internal.TraceDispatcher{nil, typedNil} {
		installTraceInterceptor(&options, unavailable)
	}
	require.Same(t, current, options.TraceDispatcher)
	require.Equal(t, 1, old.closes)
	require.Equal(t, 1, middle.closes)
	require.Zero(t, current.closes)
	for _, dispatcher := range []*replacementTraceDispatcher{old, middle, current} {
		require.Equal(t, 1, dispatcher.starts)
	}
	ctx := primitive.WithProducerCtx(context.Background(), &primitive.ProducerCtx{
		ProducerGroup: "group", Message: *primitive.NewMessage("topic", []byte("payload")),
	})
	reply := &primitive.SendResult{RegionID: "region", TraceOn: true, Status: primitive.SendOK}
	businessError := errors.New("business error")
	err := primitive.ChainInterceptors(options.Interceptors...)(ctx, nil, reply,
		func(context.Context, interface{}, interface{}) error {
			calls = append(calls, "business")
			return businessError
		})
	require.Equal(t, businessError, err)
	require.Equal(t, []string{"first", "second", "business"}, calls)
	require.Empty(t, old.records, "replaced dispatcher must not receive trace records")
	require.Empty(t, middle.records, "repeated replacement must remove each stale interceptor")
	require.Len(t, current.records, 1)
	require.Len(t, options.Interceptors, 3)
}
