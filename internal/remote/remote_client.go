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
	"io"
	"net"
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/primitive"

	"github.com/apache/rocketmq-client-go/rlog"
)

type ClientRequestFunc func(*RemotingCommand) *RemotingCommand

type TcpOption struct {
	// TODO
}

type RemotingClient struct {
	responseTable    sync.Map
	connectionTable  sync.Map
	option           TcpOption
	processors       map[int16]ClientRequestFunc
	connectionLocker sync.Mutex
	interceptor      primitive.Interceptor
}

func NewRemotingClient() *RemotingClient {
	return &RemotingClient{
		processors: make(map[int16]ClientRequestFunc),
	}
}

func (c *RemotingClient) RegisterRequestFunc(code int16, f ClientRequestFunc) {
	c.processors[code] = f
}

// TODO: merge sync and async model. sync should run on async model by blocking on chan
func (c *RemotingClient) InvokeSync(addr string, request *RemotingCommand, timeout time.Duration) (*RemotingCommand, error) {
	conn, err := c.connect(addr)
	if err != nil {
		return nil, err
	}
	resp := NewResponseFuture(request.Opaque, timeout, nil)
	c.responseTable.Store(resp.Opaque, resp)
	defer c.responseTable.Delete(request.Opaque)
	err = c.sendRequest(conn, request)
	if err != nil {
		return nil, err
	}
	resp.SendRequestOK = true
	return resp.waitResponse()
}

// InvokeAsync send request without blocking, just return immediately.
func (c *RemotingClient) InvokeAsync(addr string, request *RemotingCommand, timeout time.Duration, callback func(*ResponseFuture)) error {
	conn, err := c.connect(addr)
	if err != nil {
		return err
	}
	resp := NewResponseFuture(request.Opaque, timeout, callback)
	c.responseTable.Store(resp.Opaque, resp)
	err = c.sendRequest(conn, request)
	if err != nil {
		return err
	}
	resp.SendRequestOK = true
	go c.receiveAsync(resp)
	return nil
}

func (c *RemotingClient) receiveAsync(f *ResponseFuture) {
	_, err := f.waitResponse()
	if err != nil {
		f.executeInvokeCallback()
	}
}

func (c *RemotingClient) InvokeOneWay(addr string, request *RemotingCommand, timeout time.Duration) error {
	conn, err := c.connect(addr)
	if err != nil {
		return err
	}
	return c.sendRequest(conn, request)
}

func (c *RemotingClient) connect(addr string) (net.Conn, error) {
	//it needs additional locker.
	c.connectionLocker.Lock()
	defer c.connectionLocker.Unlock()
	conn, ok := c.connectionTable.Load(addr)
	if ok {
		return conn.(net.Conn), nil
	}
	tcpConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	c.connectionTable.Store(addr, tcpConn)
	go c.receiveResponse(tcpConn)
	return tcpConn, nil
}

func (c *RemotingClient) receiveResponse(r net.Conn) {
	scanner := c.createScanner(r)
	for scanner.Scan() {
		cmd, err := decode(scanner.Bytes())
		if err != nil {
			c.closeConnection(r)
			rlog.Errorf("decode RemotingCommand error: %s", err.Error())
			break
		}
		if cmd.isResponseType() {
			resp, exist := c.responseTable.Load(cmd.Opaque)
			if exist {
				c.responseTable.Delete(cmd.Opaque)
				responseFuture := resp.(*ResponseFuture)
				go func() {
					responseFuture.ResponseCommand = cmd
					responseFuture.executeInvokeCallback()
					if responseFuture.Done != nil {
						responseFuture.Done <- true
					}
				}()
			}
		} else {
			f := c.processors[cmd.Code]
			if f != nil {
				go func() { // 单个goroutine会造成死锁
					res := f(cmd)
					if res != nil {
						err := c.sendRequest(r, res)
						if err != nil {
							rlog.Warnf("send response to broker error: %s, type is: %d", err, res.Code)
						}
					}
				}()
			} else {
				rlog.Warnf("receive broker's requests, but no func to handle, code is: %d", cmd.Code)
			}
		}
	}
	if scanner.Err() != nil {
		rlog.Errorf("net: %s scanner exit, Err: %s.", r.RemoteAddr().String(), scanner.Err())
	} else {
		rlog.Infof("net: %s scanner exit.", r.RemoteAddr().String())
	}
}

func (c *RemotingClient) createScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		defer func() {
			if err := recover(); err != nil {
				rlog.Errorf("panic: %v", err)
			}
		}()
		if !atEOF {
			if len(data) >= 4 {
				var length int32
				err := binary.Read(bytes.NewReader(data[0:4]), binary.BigEndian, &length)
				if err != nil {
					rlog.Errorf("split data error: %s", err.Error())
					return 0, nil, err
				}

				if int(length)+4 <= len(data) {
					return int(length) + 4, data[4 : length+4], nil
				}
			}
		}
		return 0, nil, nil
	})
	return scanner
}

func (c *RemotingClient) sendRequest(conn net.Conn, request *RemotingCommand) error {
	var err error
	if c.interceptor != nil {
		err = c.interceptor(context.Background(), request, nil, func(ctx context.Context, req, reply interface{}) error {
			return c.doRequest(conn, request)
		})
	} else {
		err = c.doRequest(conn, request)
	}
	return err
}

func (c *RemotingClient) doRequest(conn net.Conn, request *RemotingCommand) error {
	content, err := encode(request)
	if err != nil {
		return err
	}
	_, err = conn.Write(content)
	if err != nil {
		c.closeConnection(conn)
		return err
	}
	return nil
}

func (c *RemotingClient) closeConnection(toCloseConn net.Conn) {
	c.connectionTable.Range(func(key, value interface{}) bool {
		if value == toCloseConn {
			c.connectionTable.Delete(key)
			return false
		} else {
			return true
		}
	})
}

func (c *RemotingClient) ShutDown() {
	c.responseTable.Range(func(key, value interface{}) bool {
		c.responseTable.Delete(key)
		return true
	})
	c.connectionTable.Range(func(key, value interface{}) bool {
		conn := value.(net.Conn)
		conn.Close()
		return true
	})
}

func (c *RemotingClient) RegisterInterceptor(interceptors ...primitive.Interceptor) {
	if len(interceptors) == 0 {
		return
	}
	idx := 0
	if c.interceptor == nil {
		c.interceptor = interceptors[0]
		idx = 1
	}
	for ; idx < len(interceptors); idx++ {
		c.interceptor = func(ctx context.Context, req, reply interface{}, invoker primitive.Invoker) error {
			return interceptors[0](ctx, req, reply, getChainedInterceptor(interceptors, idx, invoker))
		}
	}
}

// TODO
// getChainedInterceptor recursively generate the chained invoker.
func getChainedInterceptor(interceptors []primitive.Interceptor, cur int, finalInvoker primitive.Invoker) primitive.Invoker {
	if cur == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx context.Context, req, reply interface{}) error {
		return interceptors[cur+1](ctx, req, reply, getChainedInterceptor(interceptors, cur+1, finalInvoker))
	}
}
