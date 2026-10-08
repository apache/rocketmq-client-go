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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type mutableTraceResolver struct {
	mu    sync.Mutex
	addrs []string
}

func (r *mutableTraceResolver) Resolve() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.addrs...)
}
func (r *mutableTraceResolver) Description() string { return "test discovery endpoint" }
func (r *mutableTraceResolver) set(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addrs = []string{addr}
}

func traceTestShared(key string, resolver primitive.NsResolver, created, closed *int32) primitive.SharedTraceClientConfig {
	return primitive.SharedTraceClientConfig{Key: key, ResolverFactory: func() (primitive.NsResolver, func(), error) {
		atomic.AddInt32(created, 1)
		return resolver, func() { atomic.AddInt32(closed, 1) }, nil
	}}
}

func TestSharedTraceDiscoveryAndLifetime(t *testing.T) {
	var created, closed int32
	resolver := &mutableTraceResolver{addrs: []string{"127.0.0.1:9876"}}
	shared := traceTestShared(t.Name(), resolver, &created, &closed)
	first := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, first)
	defer first.Close()
	first.Start()
	first.Start()
	resolver.set("127.0.0.2:9876")
	second := NewSharedTraceDispatcher(&primitive.TraceConfig{GroupName: "different-consumer"}, shared)
	require.NotNil(t, second)
	defer second.Close()
	require.Same(t, first.resource, second.resource)
	require.Same(t, first.namesrvs, second.namesrvs)
	require.Same(t, first.cli.GetNameSrv(), second.namesrvs)
	require.Equal(t, int32(1), atomic.LoadInt32(&created))
	first.namesrvs.UpdateNameServerAddress()
	require.Equal(t, []string{"127.0.0.2:9876"}, second.namesrvs.AddrList())
	first.Close()
	first.Close()
	require.Equal(t, int32(0), atomic.LoadInt32(&closed))
	second.Start()
	require.True(t, second.Append(TraceContext{}))
	second.Close()
	require.Equal(t, int32(1), atomic.LoadInt32(&closed))
	require.False(t, first.Append(TraceContext{}))
	first.Start()
	resolver.set("127.0.0.3:9876")
	third := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, third)
	defer third.Close()
	require.NotSame(t, first.resource, third.resource)
	require.Equal(t, []string{"127.0.0.3:9876"}, third.namesrvs.AddrList())
	third.Close() // Closing before Start must release ownership too.
	require.Equal(t, int32(2), atomic.LoadInt32(&created))
	require.Equal(t, int32(2), atomic.LoadInt32(&closed))
}

func TestSharedTraceIdentityIsolation(t *testing.T) {
	var created, closed int32
	resolver := primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"})
	shared := traceTestShared(t.Name(), resolver, &created, &closed)
	base := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, base)
	defer base.Close()
	for _, cfg := range []primitive.TraceConfig{
		{UnitName: "other-unit"}, {Access: primitive.Cloud},
		{Credentials: primitive.Credentials{AccessKey: "user", SecretKey: "secret"}},
		{Credentials: primitive.Credentials{AccessKey: "user", SecretKey: "rotated", SecurityToken: "token"}},
	} {
		other := NewSharedTraceDispatcher(&cfg, shared)
		require.NotNil(t, other)
		require.NotSame(t, base.resource, other.resource)
		other.Close()
	}
	shared.Key += "-other-cluster"
	other := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, other)
	require.NotSame(t, base.resource, other.resource)
	other.Close()
	base.Close()
	require.Equal(t, int32(6), atomic.LoadInt32(&created))
	require.Equal(t, atomic.LoadInt32(&created), atomic.LoadInt32(&closed))
}

func TestTraceLegacyMismatchAndRestart(t *testing.T) {
	cfg := &primitive.TraceConfig{UnitName: t.Name(), NamesrvAddrs: []string{"127.0.0.1:9876"}}
	first := NewTraceDispatcher(cfg)
	require.NotNil(t, first)
	defer first.Close()
	second := NewTraceDispatcher(cfg)
	require.NotNil(t, second)
	defer second.Close()
	require.Same(t, first.namesrvs, second.namesrvs)
	cfg.NamesrvAddrs = []string{"127.0.0.2:9876"}
	missing := NewTraceDispatcher(cfg)
	require.Nil(t, missing)
	require.True(t, IsNilTraceDispatcher(missing))
	missing.Start()
	missing.Close()
	require.False(t, missing.Append(TraceContext{}))
	first.Close()
	second.Close()
	replacement := NewTraceDispatcher(cfg)
	require.NotNil(t, replacement)
	defer replacement.Close()
	require.NotSame(t, first.resource, replacement.resource)
	require.Equal(t, cfg.NamesrvAddrs, replacement.namesrvs.AddrList())
}

func TestSharedTraceFactoryFailures(t *testing.T) {
	var created, closed int32
	require.Nil(t, NewTraceDispatcher(nil))
	require.Nil(t, NewTraceDispatcher(&primitive.TraceConfig{}))
	require.Nil(t, NewSharedTraceDispatcher(&primitive.TraceConfig{}, primitive.SharedTraceClientConfig{}))
	for _, mode := range []string{"error", "nil", "invalid", "empty"} {
		t.Run(mode, func(t *testing.T) {
			shared := primitive.SharedTraceClientConfig{Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
				atomic.AddInt32(&created, 1)
				cleanup := func() { atomic.AddInt32(&closed, 1) }
				switch mode {
				case "error":
					return nil, cleanup, errors.New("discovery unavailable")
				case "nil":
					var resolver *mutableTraceResolver
					return resolver, cleanup, nil
				case "invalid":
					return primitive.NewPassthroughResolver([]string{"invalid"}), cleanup, nil
				default:
					return &mutableTraceResolver{}, cleanup, nil
				}
			}}
			require.Nil(t, NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared))
			require.Nil(t, NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared))
		})
	}
	require.Equal(t, int32(8), created)
	require.Equal(t, created, closed)
}

func TestSharedTraceConcurrentAcquireAndClose(t *testing.T) {
	var created, closed int32
	resolver := &mutableTraceResolver{addrs: []string{"127.0.0.1:9876"}}
	shared := traceTestShared(t.Name(), resolver, &created, &closed)
	anchor := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, anchor)
	defer anchor.Close()
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				td := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
				if td == nil {
					t.Error("acquire failed")
					return
				}
				if td.resource != anchor.resource {
					t.Error("duplicate transport")
				}
				var operations sync.WaitGroup
				for k := 0; k < 3; k++ {
					operations.Add(1)
					go func() { defer operations.Done(); td.Start(); td.Append(TraceContext{}); td.Close() }()
				}
				operations.Wait()
			}
		}()
	}
	wg.Wait()
	anchor.Close()
	require.Equal(t, int32(1), atomic.LoadInt32(&created))
	require.Equal(t, int32(1), atomic.LoadInt32(&closed))
	// Exercise final release racing with a new acquisition, without an anchor.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				td := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
				if td == nil {
					t.Error("reacquire failed")
					return
				}
				td.Start()
				td.Close()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, atomic.LoadInt32(&created), atomic.LoadInt32(&closed))
	traceClients.Lock()
	_, exists := traceClients.entries[anchor.resource.key]
	traceClients.Unlock()
	require.False(t, exists)
}

func TestSharedTraceAcquireWaitsForCleanup(t *testing.T) {
	cleanupEntered, allowCleanup := make(chan struct{}), make(chan struct{})
	var created int32
	shared := primitive.SharedTraceClientConfig{Key: t.Name(), ResolverFactory: func() (primitive.NsResolver, func(), error) {
		generation := atomic.AddInt32(&created, 1)
		return primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), func() {
			if generation == 1 {
				close(cleanupEntered)
				<-allowCleanup
			}
		}, nil
	}}
	first := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, first)
	go first.Close()
	<-cleanupEntered
	acquired := make(chan *traceDispatcher, 1)
	go func() { acquired <- NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared) }()
	select {
	case <-acquired:
		t.Fatal("acquired while old resolver was closing")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowCleanup)
	second := <-acquired
	require.NotNil(t, second)
	second.Close()
	require.Equal(t, int32(2), atomic.LoadInt32(&created))
}

func testTraceRoute(broker string) *remote.RemotingCommand {
	return &remote.RemotingCommand{Code: ResSuccess, Body: []byte(fmt.Sprintf(`{"queueDatas":[{"brokerName":"broker","readQueueNums":1,"writeQueueNums":1,"perm":6}],"brokerDatas":[{"cluster":"cluster","brokerName":"broker","brokerAddrs":{"0":%q}}]}`, broker))}
}

func TestSharedTraceRefreshesBrokerRoute(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var created, closed int32
	resolver := &mutableTraceResolver{addrs: []string{"127.0.0.1:9876"}}
	shared := traceTestShared(t.Name(), resolver, &created, &closed)
	first := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, first)
	defer first.Close()
	second := NewSharedTraceDispatcher(&primitive.TraceConfig{}, shared)
	require.NotNil(t, second)
	defer second.Close()
	ns := remote.NewMockRemotingClient(ctrl)
	first.namesrvs.nameSrvClient = ns
	gomock.InOrder(
		ns.EXPECT().InvokeSync(gomock.Any(), "127.0.0.1:9876", gomock.Any()).Return(testTraceRoute("127.0.0.1:10911"), nil),
		ns.EXPECT().InvokeSync(gomock.Any(), "127.0.0.2:9876", gomock.Any()).Return(testTraceRoute("127.0.0.2:10911"), nil),
		ns.EXPECT().ShutDown(),
	)
	mq, addr := first.findMq("")
	require.NotNil(t, mq)
	require.Equal(t, "127.0.0.1:10911", addr)
	resolver.set("127.0.0.2:9876")
	first.namesrvs.UpdateNameServerAddress()
	first.resource.refreshRoutes(context.Background())
	mq, addr = second.findMq("")
	require.NotNil(t, mq)
	require.Equal(t, "127.0.0.2:10911", addr)
	// The trace-only resource doesn't need a producer/consumer registration.
	require.Nil(t, first.namesrvs.bundleClient)
}

func TestTraceCloseDrainsAndWaitsForSend(t *testing.T) {
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
	sent := make(chan func(*remote.ResponseFuture), 1)
	broker.EXPECT().InvokeAsync(gomock.Any(), "127.0.0.1:10911", gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, addr string, req *remote.RemotingCommand, cb func(*remote.ResponseFuture)) error {
			require.Contains(t, string(req.Body), "test-message")
			sent <- cb
			return nil
		})
	broker.EXPECT().ShutDown()
	ns.EXPECT().ShutDown()
	td.Start()
	require.True(t, td.Append(TraceContext{TraceType: SubBefore, TraceBeans: []TraceBean{{Topic: "topic", MsgId: "test-message"}}}))
	returned := make(chan struct{})
	go func() { td.Close(); close(returned) }()
	callback := <-sent
	select {
	case <-returned:
		t.Fatal("closed transport before callback")
	case <-time.After(20 * time.Millisecond):
	}
	callback(&remote.ResponseFuture{ResponseCommand: &remote.RemotingCommand{Code: ResSuccess}})
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("close did not finish")
	}
}

func TestTraceRouteCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ns, err := NewNamesrv(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876", "127.0.0.2:9876"}), nil)
	require.NoError(t, err)
	remoteClient := remote.NewMockRemotingClient(ctrl)
	ns.nameSrvClient = remoteClient
	ctx, cancel := context.WithCancel(context.Background())
	remoteClient.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ string, _ *remote.RemotingCommand) (*remote.RemotingCommand, error) {
			cancel()
			return nil, ctx.Err()
		})
	_, err = ns.queryTopicRouteInfoWithContext(ctx, "trace-topic")
	require.Error(t, err)
}

func TestTraceDiscoveryConcurrentReaders(t *testing.T) {
	resolver := &mutableTraceResolver{addrs: []string{"127.0.0.1:9876"}}
	td := NewTraceDispatcher(&primitive.TraceConfig{UnitName: t.Name(), Resolver: resolver})
	require.NotNil(t, td)
	defer td.Close()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			resolver.set(fmt.Sprintf("127.0.0.%d:9876", i%2+1))
			td.namesrvs.UpdateNameServerAddress()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			td.namesrvs.Size()
			td.namesrvs.String()
			td.namesrvs.getNameServerAddress()
			snapshot := td.namesrvs.AddrList()
			snapshot[0] = "must not mutate shared addresses"
		}
	}()
	wg.Wait()
	require.NotEqual(t, "must not mutate shared addresses", td.namesrvs.AddrList()[0])
}

func TestTraceSendFailuresReleaseOwnership(t *testing.T) {
	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprint(async), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			td := NewTraceDispatcher(&primitive.TraceConfig{UnitName: t.Name(), Access: primitive.Cloud, NamesrvAddrs: []string{"127.0.0.1:9876"}})
			require.NotNil(t, td)
			defer td.Close()
			ns := remote.NewMockRemotingClient(ctrl)
			td.namesrvs.nameSrvClient = ns
			broker := remote.NewMockRemotingClient(ctrl)
			td.resource.cli.remoteClient = broker
			ns.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).Return(testTraceRoute("127.0.0.1:10911"), nil)
			broker.EXPECT().InvokeAsync(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, addr string, req *remote.RemotingCommand, cb func(*remote.ResponseFuture)) error {
					err := errors.New("trace send failed")
					if async {
						cb(&remote.ResponseFuture{Err: err})
						return nil
					}
					return err
				})
			broker.EXPECT().ShutDown()
			ns.EXPECT().ShutDown()
			td.Start()
			require.True(t, td.Append(TraceContext{TraceType: SubBefore, RegionId: "region", TraceBeans: []TraceBean{{Topic: "topic", MsgId: "message"}}}))
			td.Close()
			_, ok := td.resource.topics.Load(td.GetTraceTopicName() + "region")
			require.True(t, ok)
			select {
			case <-td.closeDone:
			default:
				t.Fatal("failed send leaked the dispatcher")
			}
		})
	}
}
