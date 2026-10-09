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

package remote

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func awaitShutdownSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not release the waiting operation")
	}
}

func TestShutdownCompletesPendingRequests(t *testing.T) {
	// Reuse the process while repeatedly constructing and disposing transports.
	for cycle := 0; cycle < 10; cycle++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		client := NewRemotingClient(nil)
		defer client.ShutDown()
		requests := make(chan struct{}, 2)
		disconnected := make(chan struct{})
		go func() {
			defer close(disconnected)
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			scanner := client.createScanner(conn)
			for scanner.Scan() {
				requests <- struct{}{}
			}
		}()
		syncDone := make(chan struct{})
		go func() {
			defer close(syncDone)
			_, err := client.InvokeSync(context.Background(), listener.Addr().String(), NewRemotingCommand(10, nil, nil))
			if err == nil {
				t.Error("pending sync request succeeded after shutdown")
			}
		}()
		asyncDone := make(chan struct{})
		var callbacks int32
		err = client.InvokeAsync(context.Background(), listener.Addr().String(), NewRemotingCommand(10, nil, nil), func(f *ResponseFuture) {
			atomic.AddInt32(&callbacks, 1)
			if f.Err == nil {
				t.Error("pending async request succeeded after shutdown")
			}
			// A callback may reenter Shutdown; it must not run under the lifecycle lock.
			client.ShutDown()
			close(asyncDone)
		})
		require.NoError(t, err)
		awaitShutdownSignal(t, requests)
		awaitShutdownSignal(t, requests)
		client.ShutDown()
		awaitShutdownSignal(t, syncDone)
		awaitShutdownSignal(t, asyncDone)
		awaitShutdownSignal(t, disconnected)
		require.EqualValues(t, 1, atomic.LoadInt32(&callbacks))
		client.responseTable.Range(func(_, _ interface{}) bool { t.Error("pending response retained"); return true })
		client.connectionTable.Range(func(_, _ interface{}) bool { t.Error("connection retained"); return true })
		_, err = client.connect(context.Background(), listener.Addr().String())
		require.Equal(t, errClientClosed, err)
		err = client.InvokeAsync(context.Background(), listener.Addr().String(), NewRemotingCommand(10, nil, nil), nil)
		require.Equal(t, errClientClosed, err)
	}
}

func TestShutdownCancelsTLSHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	config := DefaultRemotingClientConfig
	config.UseTls = true
	config.ConnectionTimeout = time.Minute
	client := NewRemotingClient(&config)
	defer client.ShutDown()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	invoked := make(chan struct{})
	go func() {
		defer close(invoked)
		_, err := client.connect(context.Background(), listener.Addr().String())
		if err == nil {
			t.Error("stalled TLS handshake unexpectedly succeeded")
		}
	}()
	var conn net.Conn
	select {
	case conn = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("TLS connection was not established")
	}
	defer conn.Close()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	// Wait until TLS handshake has started. The server deliberately never replies.
	_, err = io.ReadFull(conn, make([]byte, 1))
	require.NoError(t, err)
	stopped := make(chan struct{})
	go func() { client.ShutDown(); close(stopped) }()
	awaitShutdownSignal(t, stopped)
	awaitShutdownSignal(t, invoked)
}

func TestResponseCompletionRaces(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		f := NewResponseFuture(ctx, int32(i), nil)
		cmd := NewRemotingCommand(10, nil, nil)
		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); f.complete(cmd, nil) }()
		go func() { defer wg.Done(); f.complete(nil, errClientClosed) }()
		go func() { defer wg.Done(); cancel() }()
		response, err := f.waitResponse()
		wg.Wait()
		require.True(t, response == cmd || err != nil, "completion lost both response and error")
		require.Same(t, response, f.ResponseCommand)
		require.Equal(t, err, f.Err)
	}
}

func TestConcurrentRequestRegistrationAndShutdown(t *testing.T) {
	for i := 0; i < 20; i++ {
		client := NewRemotingClient(nil)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for j := 0; j < 20; j++ {
			wg.Add(1)
			go func(id int32) {
				defer wg.Done()
				<-start
				f := NewResponseFuture(context.Background(), id, nil)
				if client.registerResponse(f) == nil {
					_, err := f.waitResponse()
					if err != errClientClosed {
						t.Error("registered request was not canceled")
					}
				}
			}(int32(j))
		}
		close(start)
		client.ShutDown()
		exited := make(chan struct{})
		go func() { wg.Wait(); close(exited) }()
		awaitShutdownSignal(t, exited)
	}
}

// The dial context covers setup only; canceling it must not close a usable TLS connection.
func TestTLSConnectionSurvivesDialContextCancellation(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "connected")
	}))
	defer server.Close()
	config := DefaultRemotingClientConfig
	config.UseTls = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := initConn(ctx, server.Listener.Addr().String(), &config)
	require.NoError(t, err)
	defer conn.destroy()
	cancel()
	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	_, err = io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "connected", string(body))
}
