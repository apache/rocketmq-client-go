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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestSharedTraceSurvivesNameServerReplacement(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var created, closed, secondFactoryCalls int32
	resolver := &mutableTraceResolver{addrs: []string{"127.0.0.1:9876"}}
	shared := traceTestShared(t.Name(), resolver, &created, &closed)
	first := NewSharedTraceDispatcher(&primitive.TraceConfig{GroupName: "old-consumer"}, shared)
	require.NotNil(t, first)
	defer first.Close()
	ns := remote.NewMockRemotingClient(ctrl)
	first.namesrvs.nameSrvClient = ns
	gomock.InOrder(
		ns.EXPECT().InvokeSync(gomock.Any(), "127.0.0.1:9876", gomock.Any()).Return(testTraceRoute("127.0.0.1:10911"), nil),
		ns.EXPECT().InvokeSync(gomock.Any(), "127.0.0.2:9876", gomock.Any()).Return(testTraceRoute("127.0.0.2:10911"), nil),
	)
	ns.EXPECT().ShutDown()
	first.Start()
	mq, addr := first.findMq("")
	require.NotNil(t, mq)
	require.Equal(t, "127.0.0.1:10911", addr)
	resolver.set("127.0.0.2:9876")
	// The new consumer has a different address snapshot but the same logical identity.
	second := NewSharedTraceDispatcher(&primitive.TraceConfig{GroupName: "new-consumer"}, primitive.SharedTraceClientConfig{
		Key: shared.Key,
		ResolverFactory: func() (primitive.NsResolver, func(), error) {
			atomic.AddInt32(&secondFactoryCalls, 1)
			return primitive.NewPassthroughResolver([]string{"127.0.0.2:9876"}), nil, nil
		},
	})
	require.NotNil(t, second)
	defer second.Close()
	require.Same(t, first.resource, second.resource)
	require.Equal(t, int32(0), atomic.LoadInt32(&secondFactoryCalls))
	first.Close()
	require.Equal(t, int32(0), atomic.LoadInt32(&closed))
	second.namesrvs.UpdateNameServerAddress()
	second.resource.refreshRoutes(context.Background())
	broker := remote.NewMockRemotingClient(ctrl)
	second.resource.cli.remoteClient = broker
	broker.EXPECT().InvokeAsync(gomock.Any(), "127.0.0.2:10911", gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, addr string, req *remote.RemotingCommand, cb func(*remote.ResponseFuture)) error {
			require.Contains(t, string(req.Body), "after-replacement")
			cb(&remote.ResponseFuture{ResponseCommand: &remote.RemotingCommand{Code: ResSuccess}})
			return nil
		})
	broker.EXPECT().ShutDown()
	second.Start()
	require.True(t, second.Append(TraceContext{TraceType: SubBefore, TraceBeans: []TraceBean{{Topic: "topic", MsgId: "after-replacement"}}}))
	second.Close()
	require.Equal(t, int32(1), atomic.LoadInt32(&closed))
	traceClients.Lock()
	_, exists := traceClients.entries[first.resource.key]
	traceClients.Unlock()
	require.False(t, exists)
}
func TestLegacyAndSharedTraceStayIsolated(t *testing.T) {
	legacy := NewTraceDispatcher(&primitive.TraceConfig{UnitName: t.Name(), NamesrvAddrs: []string{"127.0.0.1:9876"}})
	require.NotNil(t, legacy)
	defer legacy.Close()
	shared := NewSharedTraceDispatcher(&primitive.TraceConfig{UnitName: t.Name()}, primitive.SharedTraceClientConfig{
		Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
			return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), nil, nil
		},
	})
	require.NotNil(t, shared)
	defer shared.Close()
	require.NotSame(t, legacy.resource, shared.resource)
	require.NotSame(t, legacy.namesrvs, shared.namesrvs)
	legacy.Close()
	shared.Start()
	require.True(t, shared.Append(TraceContext{}))
}

func TestTraceRealSendTimeoutReleases(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buffer := make([]byte, 1024)
		for {
			if _, err := conn.Read(buffer); err != nil {
				return
			}
		}
	}()
	var created, closed int32
	td := NewSharedTraceDispatcher(&primitive.TraceConfig{}, traceTestShared(t.Name(),
		primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), &created, &closed))
	require.NotNil(t, td)
	defer td.Close()
	route := &TopicRouteData{
		QueueDataList:  []*QueueData{{BrokerName: "broker", WriteQueueNums: 1, ReadQueueNums: 1, Perm: 6}},
		BrokerDataList: []*BrokerData{{BrokerName: "broker", BrokerAddresses: map[int64]string{MasterId: listener.Addr().String()}}},
	}
	td.namesrvs.AddBroker(route)
	td.namesrvs.routeDataMap.Store(td.traceTopic, route)
	td.Start()
	require.True(t, td.Append(TraceContext{TraceType: SubBefore, TraceBeans: []TraceBean{{Topic: "topic", MsgId: "message"}}}))
	td.Close()
	select {
	case <-td.closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("send timeout did not release the resource")
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&closed))
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("broker connection was not closed")
	}
}
