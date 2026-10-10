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
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/golang/mock/gomock"

	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/require"
)

func ownershipTestOptions(t *testing.T) ClientOptions {
	options := DefaultClientOptions()
	options.InstanceName = t.Name()
	ns, err := NewNamesrv(primitive.NewPassthroughResolver([]string{"127.0.0.2:9876", "127.0.0.1:9876"}), nil)
	require.NoError(t, err)
	options.Namesrv = ns
	return options
}

func TestClientOwnershipConcurrentAcquireAndRelease(t *testing.T) {
	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				client := GetOrNewRocketMQClient(ownershipTestOptions(t), nil)
				if client == nil {
					t.Error("acquisition failed")
					return
				}
				actual := client.(*rmqClient)
				if actual.GetNameSrv().(*namesrvs).bundleClient != actual {
					t.Error("client published before initialization")
				}
				select {
				case <-actual.done:
					t.Error("acquired a closed client")
				default:
				}
				client.Shutdown()
			}
		}()
	}
	wg.Wait()
	options := ownershipTestOptions(t)
	_, exists := clientMap.Load((&rmqClient{option: options}).ClientID())
	require.False(t, exists, "all acquisitions must be released")
}

func TestClientOwnershipOldCleanupKeepsReplacement(t *testing.T) {
	options, stopServer := namesrvRebuildTestOptions(t)
	defer stopServer()
	old := GetOrNewRocketMQClient(options, nil).(*rmqClient)
	defer old.Shutdown()
	assertNamesrvRebuildRoute(t, old)
	old.Shutdown()
	replacement := GetOrNewRocketMQClient(options, nil).(*rmqClient)
	defer replacement.Shutdown()
	require.NotSame(t, old, replacement)
	old.Shutdown()
	stored, exists := clientMap.Load(replacement.ClientID())
	require.True(t, exists)
	require.Same(t, replacement, stored)
	assertNamesrvRebuildRoute(t, replacement)
	require.NotSame(t, old.GetNameSrv(), replacement.GetNameSrv())
	require.Same(t, old.GetNameSrv().(*namesrvs).remotingConfig, replacement.GetNameSrv().(*namesrvs).remotingConfig)
	require.Same(t, old, old.GetNameSrv().(*namesrvs).bundleClient)
	// Reusing the options retained inside a replacement must work as well.
	replacement.Shutdown()
	third := GetOrNewRocketMQClient(replacement.option, nil).(*rmqClient)
	defer third.Shutdown()
	assertNamesrvRebuildRoute(t, third)
}

func TestClientOwnershipKeepsResolverSnapshot(t *testing.T) {
	options := ownershipTestOptions(t)
	addresses := []string{"127.0.0.2:9876", "127.0.0.1:9876"}
	ns, err := NewNamesrv(primitive.NewPassthroughResolver(addresses), nil)
	require.NoError(t, err)
	options.Namesrv = ns
	first := GetOrNewRocketMQClient(options, nil)
	require.NotNil(t, first)
	defer first.Shutdown()
	second := GetOrNewRocketMQClient(options, nil)
	require.NotNil(t, second)
	defer second.Shutdown()
	require.Equal(t, []string{"127.0.0.2:9876", "127.0.0.1:9876"}, addresses,
		"compatibility checks must not sort a resolver-owned snapshot in place")
}

func TestClientOwnershipReplacementDuringOldShutdown(t *testing.T) {
	options, stopServer := namesrvRebuildTestOptions(t)
	defer stopServer()
	old := GetOrNewRocketMQClient(options, nil).(*rmqClient)
	defer old.Shutdown()
	assertNamesrvRebuildRoute(t, old)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	transport := remote.NewMockRemotingClient(ctrl)
	entered, resume, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	transport.EXPECT().ShutDown().Do(func() { close(entered); <-resume })
	old.remoteClient = transport
	go func() { old.Shutdown(); close(stopped) }()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }); <-stopped }
	defer release()
	<-entered
	replacement := GetOrNewRocketMQClient(options, nil).(*rmqClient)
	defer replacement.Shutdown()
	assertNamesrvRebuildRoute(t, replacement)
	release()
	assertNamesrvRebuildRoute(t, replacement)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := old.GetNameSrv().(*namesrvs).queryTopicRouteInfoWithContext(ctx, "test")
	require.Error(t, err, "the old transport must remain closed")
}

func assertNamesrvRebuildRoute(t *testing.T, client *rmqClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	route, _, err := client.GetNameSrv().(*namesrvs).updateTopicRouteInfoWithContext(ctx, "test", "", 0)
	require.NoError(t, err)
	require.Equal(t, "broker:1", route.OrderTopicConf)
}

// Reply to real framed route requests, including checking that ACL survives a rebuild.
func namesrvRebuildTestOptions(t *testing.T) (ClientOptions, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var wg sync.WaitGroup
	var conns sync.Map
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conns.Store(conn, true)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				defer conns.Delete(conn)
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				for {
					var size uint32
					if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
						return
					}
					if size < 4 || size > 1024*1024 {
						t.Error("invalid request size")
						return
					}
					frame := make([]byte, size)
					if _, err := io.ReadFull(conn, frame); err != nil {
						return
					}
					headerSize := binary.BigEndian.Uint32(frame[:4])
					if headerSize > size-4 {
						t.Error("invalid JSON header size")
						return
					}
					var request remote.RemotingCommand
					if err := json.Unmarshal(frame[4:4+headerSize], &request); err != nil {
						t.Error(err)
						return
					}
					if request.Code != ReqGetRouteInfoByTopic || request.ExtFields["topic"] != "test" ||
						request.ExtFields["AccessKey"] != "test-key" || request.ExtFields["SecurityToken"] != "test-token" || request.ExtFields["Signature"] == "" {
						t.Error("route request lost its topic or ACL credentials")
						return
					}
					response := remote.NewRemotingCommand(ResSuccess, nil, []byte(`{"orderTopicConf":"broker:1","queueDatas":[],"brokerDatas":[]}`))
					response.Opaque = request.Opaque
					response.Flag = remote.ResponseType
					if err := response.WriteTo(conn); err != nil {
						return
					}
				}
			}()
		}
	}()
	stop := func() {
		listener.Close()
		conns.Range(func(key, _ interface{}) bool { key.(net.Conn).Close(); return true })
		wg.Wait()
	}
	options := DefaultClientOptions()
	options.InstanceName = t.Name()
	config := remote.DefaultRemotingClientConfig
	config.ConnectionTimeout = time.Second
	ns, err := NewNamesrv(primitive.NewPassthroughResolver([]string{listener.Addr().String()}), &config)
	if err != nil {
		stop()
		t.Fatal(err)
	}
	ns.SetCredentials(primitive.Credentials{AccessKey: "test-key", SecretKey: "test-secret", SecurityToken: "test-token"})
	options.Namesrv = ns
	return options, stop
}

// Observe entry to Lock without relying on sleeps to schedule the clone attempt.
type snapshotAttemptLocker struct {
	sync.Locker
	attempts chan struct{}
}

func (l *snapshotAttemptLocker) Lock() {
	select {
	case l.attempts <- struct{}{}:
	default:
	}
	l.Locker.Lock()
}

type blockedDiscoveryResolver struct {
	primitive.NsResolver
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (r *blockedDiscoveryResolver) Resolve() []string {
	r.once.Do(func() { close(r.entered) })
	<-r.resume
	return r.NsResolver.Resolve()
}

func TestClientOwnershipSlowSnapshotDoesNotBlockOtherClients(t *testing.T) {
	options := ownershipTestOptions(t)
	old := GetOrNewRocketMQClient(options, nil).(*rmqClient)
	old.Shutdown()
	ns := options.Namesrv.(*namesrvs)
	resolver := &blockedDiscoveryResolver{
		NsResolver: ns.resolver, entered: make(chan struct{}), resume: make(chan struct{}),
	}
	ns.resolver = resolver
	lock := &snapshotAttemptLocker{Locker: ns.lock, attempts: make(chan struct{}, 4)}
	ns.lock = lock

	closingOptions := ownershipTestOptions(t)
	closingOptions.InstanceName += "-closing"
	closing := GetOrNewRocketMQClient(closingOptions, nil)
	defer closing.Shutdown()
	creatingOptions := ownershipTestOptions(t)
	creatingOptions.InstanceName += "-creating"
	// A competing creator uses fresh discovery state for the same client ID.
	winnerOptions := ownershipTestOptions(t)
	var workers sync.WaitGroup
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resolver.resume) }) }
	defer func() { resume(); workers.Wait() }()
	workers.Add(1)
	go func() { defer workers.Done(); ns.UpdateNameServerAddress() }()
	select {
	case <-resolver.entered:
	case <-time.After(time.Second):
		t.Fatal("discovery did not start")
	}
	<-lock.attempts // The refresh has acquired the NameServer lock.

	rebuilt := make(chan *rmqClient, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		client := GetOrNewRocketMQClient(options, nil).(*rmqClient)
		defer client.Shutdown()
		rebuilt <- client
	}()
	select {
	case <-lock.attempts: // The rebuilding client is now trying to copy the snapshot.
	case <-time.After(time.Second):
		t.Fatal("rebuild did not attempt to acquire the NameServer lock")
	}

	closed, created := make(chan struct{}), make(chan struct{})
	winnerReady, releaseWinner := make(chan *rmqClient, 1), make(chan struct{})
	defer close(releaseWinner)
	workers.Add(3)
	go func() { defer workers.Done(); closing.Shutdown(); close(closed) }()
	go func() {
		defer workers.Done()
		client := GetOrNewRocketMQClient(creatingOptions, nil)
		client.Shutdown()
		close(created)
	}()
	go func() {
		defer workers.Done()
		client := GetOrNewRocketMQClient(winnerOptions, nil).(*rmqClient)
		defer client.Shutdown()
		winnerReady <- client
		<-releaseWinner
	}()
	for name, done := range map[string]<-chan struct{}{"shutdown": closed, "creation": created} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Errorf("unrelated client %s blocked on slow discovery", name)
		}
	}
	var winner *rmqClient
	select {
	case winner = <-winnerReady:
	case <-time.After(time.Second):
		t.Error("same-ID creator with fresh discovery state was blocked")
	}
	resume()
	replacement := <-rebuilt
	if winner != nil {
		require.Same(t, winner, replacement, "recheck the client table after copying the snapshot")
		require.Same(t, winner, winner.GetNameSrv().(*namesrvs).bundleClient)
	}
	require.NotSame(t, ns, replacement.GetNameSrv())
	require.Same(t, old, ns.bundleClient, "the old NameServer binding must remain unchanged")
}

func TestClientOwnershipRebuildDoesNotRetainRetiredClients(t *testing.T) {
	client, collected := rebuiltClientWithRetiredCaches(t)
	defer client.Shutdown()
	require.Eventually(t, func() bool {
		runtime.GC()
		return len(collected) == 3
	}, 3*time.Second, 10*time.Millisecond, "the live replacement must not retain retired clients through its request handlers")
	runtime.KeepAlive(client)
}

func rebuiltClientWithRetiredCaches(t *testing.T) (*rmqClient, <-chan struct{}) {
	t.Helper()
	collected := make(chan struct{}, 3)
	client := GetOrNewRocketMQClient(ownershipTestOptions(t), nil).(*rmqClient)
	for generation := 0; generation < 3; generation++ {
		// Use a separately allocated leaf object: finalizers on the client itself
		// would interact with its intentional internal reference cycles.
		marker := new([1024]byte)
		runtime.SetFinalizer(marker, func(*[1024]byte) { collected <- struct{}{} })
		client.GetNameSrv().(*namesrvs).routeDataMap.Store("retired-cache", marker)
		options := client.option
		client.Shutdown()
		client = GetOrNewRocketMQClient(options, nil).(*rmqClient)
	}
	return client, collected
}
