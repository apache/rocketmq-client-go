/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package remote

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

var (
	//ErrRequestTimeout for request timeout error
	ErrRequestTimeout = errors.New("request timeout")
)

//ResponseFuture for
type ResponseFuture struct {
	ResponseCommand *RemotingCommand
	SendRequestOK   bool
	Err             error
	Opaque          int32
	TimeoutMillis   time.Duration
	callback        func(*ResponseFuture)
	BeginTimestamp  int64
	Done            chan bool
	callbackOnce    sync.Once
}

//NewResponseFuture create ResponseFuture with opaque, timeout and callback
func NewResponseFuture(opaque int32, timeoutMillis time.Duration, callback func(*ResponseFuture)) *ResponseFuture {
	return &ResponseFuture{
		Opaque:         opaque,
		Done:           make(chan bool),
		TimeoutMillis:  timeoutMillis,
		callback:       callback,
		BeginTimestamp: time.Now().Unix() * 1000,
	}
}

func (r *ResponseFuture) executeInvokeCallback() {
	r.callbackOnce.Do(func() {
		if r.callback != nil {
			r.callback(r)
		}
	})
}

func (r *ResponseFuture) isTimeout() bool {
	diff := time.Now().Unix()*1000 - r.BeginTimestamp
	return diff > int64(r.TimeoutMillis)
}

func (r *ResponseFuture) waitResponse() (*RemotingCommand, error) {
	for {
		select {
		case <-r.Done:
			if r.Err != nil {
				return nil, r.Err
			}
			return r.ResponseCommand, nil
		case <-time.After(r.TimeoutMillis * time.Millisecond):
			return nil, ErrRequestTimeout
		}
	}
}

//RemotingClient includes basic operations for remote
type RemotingClient interface {
	Start()
	Shutdown()
	InvokeSync(string, *RemotingCommand, time.Duration) (*RemotingCommand, error)
	InvokeAsync(string, *RemotingCommand, time.Duration, func(*ResponseFuture)) error
	InvokeOneWay(string, *RemotingCommand) error
}

//defaultRemotingClient for default RemotingClient implementation
type defaultRemotingClient struct {
	responseTable    map[int32]*ResponseFuture
	responseLock     sync.RWMutex
	connectionsTable map[string]net.Conn
	connectionLock   sync.RWMutex
	ctx              context.Context
	cancel           context.CancelFunc
}

//NewDefaultRemotingClient for
func NewDefaultRemotingClient() RemotingClient {
	client := &defaultRemotingClient{
		responseTable:    make(map[int32]*ResponseFuture, 0),
		connectionsTable: make(map[string]net.Conn, 0),
	}
	ctx, cancel := context.WithCancel(context.Background())
	client.ctx = ctx
	client.cancel = cancel
	return client
}

//Start begin sca
func (client *defaultRemotingClient) Start() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				client.scanResponseTable()
			case <-client.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

// Shutdown for call client.cancel
func (client *defaultRemotingClient) Shutdown() {
	client.cancel()
	client.connectionLock.Lock()
	for addr, conn := range client.connectionsTable {
		conn.Close()
		delete(client.connectionsTable, addr)
	}
	client.connectionLock.Unlock()
}

// InvokeSync sends request synchronously
func (client *defaultRemotingClient) InvokeSync(addr string, request *RemotingCommand, timeoutMillis time.Duration) (*RemotingCommand, error) {
	conn, err := client.connect(addr)
	if err != nil {
		return nil, err
	}
	resp := NewResponseFuture(request.Opaque, timeoutMillis, nil)
	client.responseLock.Lock()
	client.responseTable[resp.Opaque] = resp
	client.responseLock.Unlock()
	err = client.sendRequest(conn, request)
	if err != nil {
		return nil, err
	}
	resp.SendRequestOK = true
	return resp.waitResponse()
}

//InvokeAsync send request asynchronously
func (client *defaultRemotingClient) InvokeAsync(addr string, request *RemotingCommand, timeoutMillis time.Duration, callback func(*ResponseFuture)) error {
	conn, err := client.connect(addr)
	if err != nil {
		return err
	}
	resp := NewResponseFuture(request.Opaque, timeoutMillis, callback)
	client.responseLock.Lock()
	client.responseTable[resp.Opaque] = resp
	client.responseLock.Unlock()
	err = client.sendRequest(conn, request)
	if err != nil {
		return err
	}
	resp.SendRequestOK = true
	return nil
}

//InvokeOneWay send one-way request
func (client *defaultRemotingClient) InvokeOneWay(addr string, request *RemotingCommand) error {
	conn, err := client.connect(addr)
	if err != nil {
		return err
	}
	return client.sendRequest(conn, request)
}

func (client *defaultRemotingClient) scanResponseTable() {
	rfs := make([]*ResponseFuture, 0)
	client.responseLock.Lock()
	for opaque, resp := range client.responseTable {
		if (resp.BeginTimestamp + int64(resp.TimeoutMillis) + 1000) <= time.Now().Unix()*1000 {
			delete(client.responseTable, opaque)
			rfs = append(rfs, resp)
		}
	}
	client.responseLock.Unlock()
	for _, rf := range rfs {
		rf.Err = ErrRequestTimeout
		rf.executeInvokeCallback()
	}
}

func (client *defaultRemotingClient) connect(addr string) (net.Conn, error) {
	client.connectionLock.Lock()
	defer client.connectionLock.Unlock()
	conn, ok := client.connectionsTable[addr]
	if ok {
		return conn.(net.Conn), nil
	}
	tcpConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	client.connectionsTable[addr] = tcpConn
	go client.receiveResponse(tcpConn)
	return tcpConn, nil
}

func (client *defaultRemotingClient) receiveResponse(conn net.Conn) {
	scanner := createScanner(conn)
	for scanner.Scan() {
		receivedRemotingCommand, err := decode(scanner.Bytes())
		if err != nil {
			client.closeConnection(conn)
			break
		}
		if receivedRemotingCommand.isResponseType() {
			client.responseLock.Lock()
			if resp, ok := client.responseTable[receivedRemotingCommand.Opaque]; ok {
				delete(client.responseTable, receivedRemotingCommand.Opaque)
				resp.ResponseCommand = receivedRemotingCommand
				resp.executeInvokeCallback()
				if resp.Done != nil {
					resp.Done <- true
				}
			}
			client.responseLock.Unlock()
		} else {
			// todo handler request from peer
		}
	}
}

func createScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if !atEOF {
			if len(data) >= 4 {
				var length int32
				binary.Read(bytes.NewReader(data[0:4]), binary.BigEndian, &length)
				if int(length)+4 <= len(data) {
					return int(length) + 4, data[:int(length)+4], nil
				}
			}
		}
		return 0, nil, nil
	})
	return scanner
}

func (client *defaultRemotingClient) sendRequest(conn net.Conn, request *RemotingCommand) error {
	content, err := encode(request)
	if err != nil {
		return err
	}
	_, err = conn.Write(content)
	if err != nil {
		client.closeConnection(conn)
		return err
	}
	return nil
}

func (client *defaultRemotingClient) closeConnection(toCloseConn net.Conn) {
	client.connectionLock.Lock()
	var toCloseAddr string
	for addr, con := range client.connectionsTable {
		if con == toCloseConn {
			toCloseAddr = addr
			break
		}
	}
	if conn, ok := client.connectionsTable[toCloseAddr]; ok {
		delete(client.connectionsTable, toCloseAddr)
		conn.Close()
	}
	client.connectionLock.Unlock()
}
