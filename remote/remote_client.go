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
	"encoding/binary"
	"errors"
	"github.com/apache/rocketmq-client-go/rlog"
	"io"
	"net"
	"sync"
	"time"
)

var (
	//ErrRequestTimeout for request timeout error
	ErrRequestTimeout = errors.New("request timeout")
	connectionLocker  sync.Mutex
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

func InvokeSync(addr string, request *RemotingCommand, timeoutMillis time.Duration) (*RemotingCommand, error) {
	conn, err := connect(addr)
	if err != nil {
		return nil, err
	}
	resp := NewResponseFuture(request.Opaque, timeoutMillis, nil)
	responseTable.Store(resp.Opaque, resp)
	err = sendRequest(conn, request)
	if err != nil {
		return nil, err
	}
	resp.SendRequestOK = true
	return resp.waitResponse()
}

func InvokeAsync(addr string, request *RemotingCommand, timeoutMillis time.Duration, callback func(*ResponseFuture)) error {
	conn, err := connect(addr)
	if err != nil {
		return err
	}
	resp := NewResponseFuture(request.Opaque, timeoutMillis, callback)
	responseTable.Store(resp.Opaque, resp)
	err = sendRequest(conn, request)
	if err != nil {
		return err
	}
	resp.SendRequestOK = true
	return nil

}

func InvokeOneWay(addr string, request *RemotingCommand) error {
	conn, err := connect(addr)
	if err != nil {
		return err
	}
	return sendRequest(conn, request)
}

var (
	responseTable   sync.Map
	connectionTable sync.Map
)

func ScanResponseTable() {
	rfs := make([]*ResponseFuture, 0)
	responseTable.Range(func(key, value interface{}) bool {
		if resp, ok := value.(*ResponseFuture); ok {
			if (resp.BeginTimestamp + int64(resp.TimeoutMillis) + 1000) <= time.Now().Unix()*1000 {
				rfs = append(rfs, resp)
				responseTable.Delete(key)
			}
		}
		return true
	})
	for _, rf := range rfs {
		rf.Err = ErrRequestTimeout
		rf.executeInvokeCallback()
	}
}

func connect(addr string) (net.Conn, error) {
	//it needs additional locker.
	connectionLocker.Lock()
	defer connectionLocker.Unlock()
	conn, ok := connectionTable.Load(addr)
	if ok {
		return conn.(net.Conn), nil
	}
	tcpConn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	connectionTable.Store(addr, tcpConn)
	go receiveResponse(tcpConn)
	return tcpConn, nil
}

func receiveResponse(r net.Conn) {
	scanner := createScanner(r)
	for {
		scanner.Scan()
		receivedRemotingCommand, err := decode(scanner.Bytes())
		if err != nil {
			closeConnection(r)
			rlog.Error(err.Error())
			break
		}
		if receivedRemotingCommand.isResponseType() {
			resp, exist := responseTable.Load(receivedRemotingCommand.Opaque)
			if exist {
				responseTable.Delete(receivedRemotingCommand.Opaque)
				responseFuture := resp.(*ResponseFuture)
				go func() {
					responseFuture.ResponseCommand = receivedRemotingCommand
					responseFuture.executeInvokeCallback()
					if responseFuture.Done != nil {
						responseFuture.Done <- true
					}
				}()
			}
		} else {
			// todo handler request from peer
		}
	}
}

func createScanner(r io.Reader) *bufio.Scanner {
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

func sendRequest(conn net.Conn, request *RemotingCommand) error {
	content, err := encode(request)
	if err != nil {
		return err
	}
	_, err = conn.Write(content)
	if err != nil {
		closeConnection(conn)
		return err
	}
	return nil
}

func closeConnection(toCloseConn net.Conn) {
	connectionTable.Range(func(key, value interface{}) bool {
		if value == toCloseConn {
			connectionTable.Delete(key)
			return false
		} else {
			return true
		}

	})
}
