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

package internal

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestTraceRouteFailoverAfterTimeout(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		refresh := refresh
		t.Run(fmt.Sprintf("refresh=%t", refresh), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := NewTraceDispatcher(&primitive.TraceConfig{UnitName: t.Name(), NamesrvAddrs: []string{"127.0.0.1:9876", "127.0.0.2:9876"}})
			require.NotNil(t, td)
			defer td.Close()
			ns := remote.NewMockRemotingClient(ctrl)
			td.namesrvs.nameSrvClient = ns
			gomock.InOrder(
				ns.EXPECT().InvokeSync(gomock.Any(), "127.0.0.1:9876", gomock.Any()).DoAndReturn(
					func(ctx context.Context, _ string, _ *remote.RemotingCommand) (*remote.RemotingCommand, error) {
						<-ctx.Done()
						return nil, ctx.Err()
					}),
				ns.EXPECT().InvokeSync(gomock.Any(), "127.0.0.2:9876", gomock.Any()).Return(testTraceRoute("127.0.0.2:10911"), nil),
			)
			ns.EXPECT().ShutDown()
			if refresh {
				td.resource.topics.Store(td.GetTraceTopicName(), struct{}{})
				td.resource.refreshRoutes(context.Background())
				_, exists := td.namesrvs.routeDataMap.Load(td.GetTraceTopicName())
				require.True(t, exists, "refresh must reach the healthy fallback after a timeout")
			}
			mq, addr := td.findMq("")
			require.NotNil(t, mq)
			require.Equal(t, "127.0.0.2:10911", addr)
		})
	}
}

func TestTraceRouteLookupCancellation(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		t.Run(fmt.Sprintf("refresh=%t", refresh), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := NewTraceDispatcher(&primitive.TraceConfig{UnitName: t.Name(), NamesrvAddrs: []string{"127.0.0.1:9876", "127.0.0.2:9876"}})
			require.NotNil(t, td)
			defer td.Close()
			ns := remote.NewMockRemotingClient(ctrl)
			td.namesrvs.nameSrvClient = ns
			entered, returned := make(chan struct{}), make(chan struct{})
			ns.EXPECT().InvokeSync(gomock.Any(), "127.0.0.1:9876", gomock.Any()).DoAndReturn(
				func(ctx context.Context, _ string, _ *remote.RemotingCommand) (*remote.RemotingCommand, error) {
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				})
			ns.EXPECT().ShutDown()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				defer close(returned)
				if refresh {
					td.resource.topics.Store(td.GetTraceTopicName(), struct{}{})
					td.resource.refreshRoutes(ctx)
				} else {
					td.findMq("")
				}
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("route lookup did not start")
			}
			cancel()
			td.cancelSend()
			select {
			case <-returned:
			case <-time.After(time.Second):
				t.Fatal("route lookup ignored cancellation")
			}
			_, exists := td.namesrvs.routeDataMap.Load(td.GetTraceTopicName())
			require.False(t, exists)
		})
	}
}

func TestTraceBatchFlushPolicy(t *testing.T) {
	for _, tc := range []struct {
		name         string
		records      int
		idleTimeout  time.Duration
		closeToFlush bool
	}{
		{"full-batch", batchSize, time.Hour, false},
		{"idle", 3, 0, false},
		{"shutdown", 3, time.Hour, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := NewTraceDispatcher(&primitive.TraceConfig{UnitName: t.Name(), NamesrvAddrs: []string{"127.0.0.1:9876"}})
			require.NotNil(t, td)
			defer td.Close()
			ns := remote.NewMockRemotingClient(ctrl)
			td.namesrvs.nameSrvClient = ns
			broker := remote.NewMockRemotingClient(ctrl)
			td.resource.cli.remoteClient = broker
			ns.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).Return(testTraceRoute("127.0.0.1:10911"), nil)
			ns.EXPECT().ShutDown()
			broker.EXPECT().ShutDown()
			sent := make(chan string, 1)
			broker.EXPECT().InvokeAsync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, addr string, req *remote.RemotingCommand, callback func(*remote.ResponseFuture)) error {
					sent <- string(req.Body)
					callback(&remote.ResponseFuture{ResponseCommand: &remote.RemotingCommand{Code: ResSuccess}})
					return nil
				}).Times(1)

			// Drive ticker events explicitly. A long idle threshold lets us test active
			// traffic without depending on the scheduler meeting a 5ms deadline.
			ticks := make(chan time.Time)
			td.input = make(chan TraceContext)
			td.ticker = time.NewTicker(time.Hour)
			td.ticker.C = ticks
			td.started = true
			go func() {
				defer close(td.processDone)
				td.process(tc.idleTimeout)
			}()
			for i := 0; i < tc.records; i++ {
				td.input <- TraceContext{TraceType: SubBefore, TraceBeans: []TraceBean{{Topic: "topic", MsgId: "batch-record"}}}
				if tc.name == "full-batch" && i < tc.records-1 {
					ticks <- time.Now()
				}
			}
			if tc.closeToFlush {
				td.Close()
			} else if tc.name == "idle" {
				ticks <- time.Now()
			}
			select {
			case body := <-sent:
				require.Equal(t, tc.records, strings.Count(body, "batch-record"))
			case <-time.After(time.Second):
				t.Fatal("trace batch was not sent")
			}
			td.Close()
		})
	}
}
