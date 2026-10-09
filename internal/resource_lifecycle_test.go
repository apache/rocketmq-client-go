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
	"bytes"
	"context"
	"errors"
	"github.com/golang/mock/gomock"
	"io/ioutil"
	"net"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/apache/rocketmq-client-go/v2/internal/remote"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/stretchr/testify/require"
)

func TestLastClientReleaseClosesNameServerConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	disconnected := make(chan struct{})
	go func() {
		defer close(disconnected)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		accepted <- conn
		ioutil.ReadAll(conn)
	}()
	srvs, err := NewNamesrv(primitive.NewPassthroughResolver([]string{listener.Addr().String()}), nil)
	require.NoError(t, err)
	options := DefaultClientOptions()
	options.InstanceName = t.Name()
	options.Namesrv = srvs
	first := GetOrNewRocketMQClient(options, nil)
	second := GetOrNewRocketMQClient(options, nil)
	require.NotNil(t, first)
	require.Same(t, first, second)
	defer second.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = srvs.nameSrvClient.InvokeOneWay(ctx, listener.Addr().String(), remote.NewRemotingCommand(ReqGetRouteInfoByTopic, nil, nil))
	require.NoError(t, err)
	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-ctx.Done():
		t.Fatal("no NameServer connection was opened")
	}
	first.Shutdown()
	// A remaining owner must still be able to use the connection.
	err = srvs.nameSrvClient.InvokeOneWay(ctx, listener.Addr().String(), remote.NewRemotingCommand(ReqGetRouteInfoByTopic, nil, nil))
	require.NoError(t, err)
	second.Shutdown()
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("NameServer connection remained open after the last release")
	}
	// Late refresh work must not recreate the disposed transport's connection.
	err = srvs.nameSrvClient.InvokeOneWay(ctx, listener.Addr().String(), remote.NewRemotingCommand(ReqGetRouteInfoByTopic, nil, nil))
	require.Error(t, err)
}

func TestConcurrentShutdownAndInvoke(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	r := remote.NewMockRemotingClient(ctrl)
	r.EXPECT().InvokeSync(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("test transport")).AnyTimes()
	r.EXPECT().ShutDown()
	opts := DefaultClientOptions()
	opts.InstanceName = t.Name()
	srvs, err := NewNamesrv(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), nil)
	require.NoError(t, err)
	opts.Namesrv = srvs
	c := GetOrNewRocketMQClient(opts, nil).(*rmqClient)
	c.remoteClient = r
	started, stop, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				c.InvokeSync(context.Background(), "unused", remote.NewRemotingCommand(10, nil, nil), time.Second)
			}
		}
	}()
	<-started
	time.Sleep(time.Millisecond)
	c.Shutdown()
	close(stop)
	<-stopped
}

func clientBackgroundWorkers() int {
	var b bytes.Buffer
	pprof.Lookup("goroutine").WriteTo(&b, 2)
	return strings.Count(b.String(), "(*rmqClient).Start.func")
}

func TestClientRepeatedLifecycleStopsBackgroundWorkers(t *testing.T) {
	baseline := clientBackgroundWorkers()
	for i := 0; i < 10; i++ {
		srvs, err := NewNamesrv(primitive.NewPassthroughResolver([]string{"127.0.0.1:9876"}), nil)
		require.NoError(t, err)
		opts := DefaultClientOptions()
		opts.InstanceName = t.Name()
		opts.Namesrv = srvs
		c := GetOrNewRocketMQClient(opts, nil)
		c.Start()
		c.Shutdown()
		require.Eventually(t, func() bool { return clientBackgroundWorkers() <= baseline }, time.Second, time.Millisecond, "shutdown left discovery/heartbeat workers waiting")
	}
}
