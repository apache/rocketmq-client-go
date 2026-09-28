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
