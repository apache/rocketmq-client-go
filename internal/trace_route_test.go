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
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const traceRouteTestBrokers = 8

func multiBrokerTraceRoute(generation int) *TopicRouteData {
	route := &TopicRouteData{}
	// Deliberately return reverse order so publish queue selection must sort.
	for i := traceRouteTestBrokers - 1; i >= 0; i-- {
		name := fmt.Sprintf("broker-%02d", i)
		route.QueueDataList = append(route.QueueDataList, &QueueData{
			BrokerName: name, ReadQueueNums: 1, WriteQueueNums: 1, Perm: 6,
		})
		route.BrokerDataList = append(route.BrokerDataList, &BrokerData{
			Cluster: "cluster", BrokerName: name,
			BrokerAddresses: map[int64]string{MasterId: fmt.Sprintf("127.0.0.%d:%d", i+1, 10911+generation%2)},
		})
	}
	return route
}

func TestSharedTraceConcurrentColdRouteLookup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var created, closed int32
	shared := traceTestShared(t.Name(), primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), &created, &closed)
	const callers = 24
	dispatchers := make([]*traceDispatcher, callers)
	for i := range dispatchers {
		dispatchers[i] = NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
		require.NotNil(t, dispatchers[i])
		defer dispatchers[i].Close()
	}
	ns := remote.NewMockRemotingClient(ctrl)
	dispatchers[0].namesrvs.nameSrvClient = ns
	entered, release := make(chan struct{}), make(chan struct{})
	ns.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, *remote.RemotingCommand) (*remote.RemotingCommand, error) {
			close(entered)
			<-release
			return testTraceRoute("127.0.0.1:10911"), nil
		}).Times(1)
	ns.EXPECT().ShutDown()
	var wg sync.WaitGroup
	lookup := func(td *traceDispatcher) {
		defer wg.Done()
		mq, addr := td.findMq("")
		if mq == nil || addr != "127.0.0.1:10911" {
			t.Errorf("incomplete shared route: queue=%v, address=%q", mq, addr)
		}
	}
	wg.Add(1)
	go lookup(dispatchers[0])
	<-entered
	started := make(chan struct{}, callers-1)
	for _, td := range dispatchers[1:] {
		wg.Add(1)
		go func(td *traceDispatcher) { started <- struct{}{}; lookup(td) }(td)
	}
	for i := 1; i < callers; i++ {
		<-started
	}
	// Keep the first RPC pending while the other callers observe the cold cache.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
}

func TestTraceFailedRouteLookupCanRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ns, err := NewNamesrv(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), nil)
	require.NoError(t, err)
	remoteClient := remote.NewMockRemotingClient(ctrl)
	ns.nameSrvClient = remoteClient
	gomock.InOrder(
		remoteClient.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, context.DeadlineExceeded),
		remoteClient.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).Return(testTraceRoute("127.0.0.1:10911"), nil),
		remoteClient.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).Return(testTraceRoute("127.0.0.2:10911"), nil),
	)
	_, err = ns.fetchPublishMessageQueuesWithContext(context.Background(), "trace")
	require.Error(t, err)
	queues, err := ns.fetchPublishMessageQueuesWithContext(context.Background(), "trace")
	require.NoError(t, err)
	require.Len(t, queues, 1)
	_, _, err = ns.updateTopicRouteInfoWithContext(context.Background(), "trace", "", 0)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.2:10911", ns.FindBrokerAddrByName("broker"), "explicit refresh must bypass the cached route")
}

func TestTraceNameServerRefreshWhileRouteQueryBlocked(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	resolver := &mutableTraceResolver{addrs: []string{"127.0.0.1:9876"}}
	ns, err := NewNamesrv(resolver, nil)
	require.NoError(t, err)
	remoteClient := remote.NewMockRemotingClient(ctrl)
	ns.nameSrvClient = remoteClient
	client := &traceClient{namesrvs: ns, done: make(chan struct{})}
	client.topics.Store("trace", struct{}{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	remoteClient.EXPECT().InvokeSync(gomock.Any(), "127.0.0.1:9876", gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ string, _ *remote.RemotingCommand) (*remote.RemotingCommand, error) {
			close(entered)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	go client.runRefresh(ctx, time.Millisecond, time.Millisecond, time.Millisecond)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("route query did not start")
	}
	resolver.set("127.0.0.2:9876")
	require.Eventually(t, func() bool { return ns.AddrList()[0] == "127.0.0.2:9876" }, time.Second, time.Millisecond,
		"a blocked route RPC must not delay address discovery")
	cancel()
	select {
	case <-client.done:
	case <-time.After(time.Second):
		t.Fatal("refresh workers did not exit after cancellation")
	}
}

func TestTraceRoutePublishesBrokerBeforeQueues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var created, closed int32
	td := NewSharedTraceDispatcher(&primitive.TraceConfig{}, traceTestShared(t.Name(),
		primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), &created, &closed))
	require.NotNil(t, td)
	defer td.Close()
	ns := remote.NewMockRemotingClient(ctrl)
	td.namesrvs.nameSrvClient = ns
	generation := 0
	ns.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, *remote.RemotingCommand) (*remote.RemotingCommand, error) {
			generation++
			name := fmt.Sprintf("broker-%d", generation)
			route := &TopicRouteData{
				QueueDataList:  []*QueueData{{BrokerName: name, ReadQueueNums: 1, WriteQueueNums: 1, Perm: 6}},
				BrokerDataList: []*BrokerData{{BrokerName: name, BrokerAddresses: map[int64]string{MasterId: "127.0.0.1:10911"}}},
			}
			return &remote.RemotingCommand{Code: ResSuccess, Body: []byte(route.String())}, nil
		}).AnyTimes()
	ns.EXPECT().ShutDown()
	mq, addr := td.findMq("")
	require.NotNil(t, mq)
	require.NotEmpty(t, addr)
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 200; i++ {
			td.resource.refreshRoutes(context.Background())
		}
	}()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				mq, addr := td.findMq("")
				if mq == nil || addr == "" {
					t.Errorf("visible queue has no broker address: %v", mq)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestTraceCachedRoutePreservesSnapshot(t *testing.T) {
	ns, err := NewNamesrv(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), nil)
	require.NoError(t, err)
	defer ns.nameSrvClient.ShutDown()
	route := multiBrokerTraceRoute(0)
	before := append([]*QueueData(nil), route.QueueDataList...)
	ns.routeDataMap.Store("trace-topic", route)
	queues, err := ns.fetchPublishMessageQueuesWithContext(context.Background(), "trace-topic")
	require.NoError(t, err)
	require.Len(t, queues, traceRouteTestBrokers)
	for i, queue := range queues {
		require.Equal(t, fmt.Sprintf("broker-%02d", i), queue.BrokerName)
	}
	require.Equal(t, before, route.QueueDataList, "publish queue sorting must not reorder the cached snapshot")
}

func TestSharedTraceConcurrentMultiBrokerRouteRefresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var created, closed int32
	shared := traceTestShared(t.Name(), primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), &created, &closed)
	first := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, first)
	defer first.Close()
	second := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, second)
	defer second.Close()
	require.Same(t, first.namesrvs, second.namesrvs)
	topic := first.GetTraceTopicName()
	first.namesrvs.routeDataMap.Store(topic, multiBrokerTraceRoute(0))
	first.resource.topics.Store(topic, struct{}{})

	ns := remote.NewMockRemotingClient(ctrl)
	first.namesrvs.nameSrvClient = ns
	generation := 0
	const refreshes = 200
	ns.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, string, *remote.RemotingCommand) (*remote.RemotingCommand, error) {
			generation++
			return &remote.RemotingCommand{Code: ResSuccess, Body: []byte(multiBrokerTraceRoute(generation).String())}, nil
		}).Times(refreshes)
	ns.EXPECT().ShutDown()

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < refreshes; i++ {
			first.resource.refreshRoutes(context.Background())
		}
	}()
	for i := 0; i < 4; i++ {
		dispatcher := []*traceDispatcher{first, second}[i%2]
		wg.Add(1)
		go func(td *traceDispatcher) {
			defer wg.Done()
			<-start
			for j := 0; j < 1000; j++ {
				queues, err := td.namesrvs.fetchPublishMessageQueuesWithContext(context.Background(), topic)
				if err != nil || len(queues) != traceRouteTestBrokers {
					t.Errorf("incomplete route snapshot: %d queues, error %v", len(queues), err)
					return
				}
				for k, queue := range queues {
					if queue.BrokerName != fmt.Sprintf("broker-%02d", k) || queue.QueueId != 0 {
						t.Errorf("inconsistent route snapshot at queue %d: %+v", k, queue)
						return
					}
				}
			}
		}(dispatcher)
	}
	close(start)
	wg.Wait()
}
