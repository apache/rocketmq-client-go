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
	"bytes"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestNewResponseFuture(t *testing.T) {
	future := NewResponseFuture(10, time.Duration(1000), nil)
	if future.Opaque != 10 {
		t.Errorf("wrong ResponseFuture's Opaque. want=%d, got=%d", 10, future.Opaque)
	}
	if future.SendRequestOK != false {
		t.Errorf("wrong ResposneFutrue's SendRequestOK. want=%t, got=%t", false, future.SendRequestOK)
	}
	if future.Err != nil {
		t.Errorf("wrong RespnseFuture's Err. want=<nil>, got=%v", future.Err)
	}
	if future.TimeoutMillis != time.Duration(1000) {
		t.Errorf("wrong ResponseFuture's TimeoutMills. want=%d, got=%d",
			future.TimeoutMillis, time.Duration(1000))
	}
	if future.callback != nil {
		t.Errorf("wrong ResponseFuture's callback. want=<nil>, got=%v", future.callback)
	}
	if future.Done == nil {
		t.Errorf("wrong ResponseFuture's Done. want=<channel>, got=<nil>")
	}
}

func TestResponseFutureTimeout(t *testing.T) {
	callback := func(r *ResponseFuture) {
		if r.ResponseCommand.Remark == "" {
			r.ResponseCommand.Remark = "Hello RocketMQ."
		} else {
			r.ResponseCommand.Remark = r.ResponseCommand.Remark + "Go Client"
		}
	}
	future := NewResponseFuture(10, time.Duration(1000), callback)
	future.ResponseCommand = NewRemotingCommand(200,
		nil, nil)

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func() {
			future.executeInvokeCallback()
			wg.Done()
		}()
	}
	wg.Wait()
	if future.ResponseCommand.Remark != "Hello RocketMQ." {
		t.Errorf("wrong ResponseFuture.ResponseCommand.Remark. want=%s, got=%s",
			"Hello RocketMQ.", future.ResponseCommand.Remark)
	}

}

func TestResponseFutureIsTimeout(t *testing.T) {
	future := NewResponseFuture(10, time.Duration(500), nil)
	if future.isTimeout() != false {
		t.Errorf("wrong ResponseFuture's istimeout. want=%t, got=%t", false, future.isTimeout())
	}
	time.Sleep(time.Duration(2000) * time.Millisecond)
	if future.isTimeout() != true {
		t.Errorf("wrong ResponseFuture's istimeout. want=%t, got=%t", true, future.isTimeout())
	}

}

func TestResponseFutureWaitResponse(t *testing.T) {
	future := NewResponseFuture(10, time.Duration(500), nil)
	if _, err := future.waitResponse(); err != ErrRequestTimeout {
		t.Errorf("wrong ResponseFuture waitResponse. want=%v, got=%v",
			ErrRequestTimeout, err)
	}
	future = NewResponseFuture(10, time.Duration(500), nil)
	responseError := errors.New("response error")
	go func() {
		time.Sleep(100 * time.Millisecond)
		future.Err = responseError
		future.Done <- true
	}()
	if _, err := future.waitResponse(); err != responseError {
		t.Errorf("wrong ResponseFuture waitResponse. want=%v. got=%v",
			responseError, err)
	}
	future = NewResponseFuture(10, time.Duration(500), nil)
	responseRemotingCommand := NewRemotingCommand(202, nil, nil)
	go func() {
		time.Sleep(100 * time.Millisecond)
		future.ResponseCommand = responseRemotingCommand
		future.Done <- true
	}()
	if r, err := future.waitResponse(); err != nil {
		t.Errorf("wrong ResponseFuture waitResponse error: %v", err)
	} else {
		if r != responseRemotingCommand {
			t.Errorf("wrong ResponseFuture waitResposne result. want=%v, got=%v",
				responseRemotingCommand, r)
		}
	}
}

func TestCreateScanner(t *testing.T) {
	r := randomNewRemotingCommand()
	content, err := encode(r)
	if err != nil {
		t.Fatalf("failed to encode RemotingCommand. %s", err)
	}
	reader := bytes.NewReader(content)
	scanner := createScanner(reader)
	for scanner.Scan() {
		rcr, err := decode(scanner.Bytes())
		if err != nil {
			t.Fatalf("failedd to decode RemotingCommand from scanner")
		}
		if !reflect.DeepEqual(*r, *rcr) {
			t.Fatal("decoded RemotingCommand not equal to the original one")
		}
	}
}

func TestInvokeSync(t *testing.T) {
	clientSendRemtingCommand := NewRemotingCommand(10, nil, []byte("Hello RocketMQ"))
	serverSendRemotingCommand := NewRemotingCommand(20, nil, []byte("Welcome native"))
	serverSendRemotingCommand.Opaque = clientSendRemtingCommand.Opaque
	serverSendRemotingCommand.Flag = ResponseType
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		receiveCommand, err := InvokeSync(":3000",
			clientSendRemtingCommand, time.Duration(1000))
		if err != nil {
			t.Fatalf("failed to invoke synchronous. %s", err)
		} else {
			if !reflect.DeepEqual(&receiveCommand, &serverSendRemotingCommand) {
				t.Errorf("remotingCommand prased in client is different from server. ")
			}
		}
		wg.Done()
	}()

	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := createScanner(conn)
		for scanner.Scan() {
			receivedRemotingCommand, err := decode(scanner.Bytes())
			if err != nil {
				t.Errorf("failed to decode RemotingCommnad. %s", err)
			}
			if clientSendRemtingCommand.Code != receivedRemotingCommand.Code {
				t.Errorf("wrong code. want=%d, got=%d", receivedRemotingCommand.Code,
					clientSendRemtingCommand.Code)
			}
			body, err := encode(serverSendRemotingCommand)
			if err != nil {
				t.Fatalf("failed to encode RemotingCommand")
			}
			_, err = conn.Write(body)
			if err != nil {
				t.Fatalf("failed to write body to conneciton.")
			}
			return
		}
	}
	wg.Done()
}

func TestInvokeAsync(t *testing.T) {
	clientSendRemtingCommand := NewRemotingCommand(10, nil, []byte("Hello RocketMQ"))
	serverSendRemotingCommand := NewRemotingCommand(20, nil, []byte("Welcome native"))
	serverSendRemotingCommand.Opaque = clientSendRemtingCommand.Opaque
	serverSendRemotingCommand.Flag = ResponseType

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := InvokeAsync(":3000", clientSendRemtingCommand,
			time.Duration(1000), func(r *ResponseFuture) {
				if string(r.ResponseCommand.Body) != "Welcome native" {
					t.Errorf("wrong responseCommand.Body. want=%s, got=%s",
						"Welcome native", string(r.ResponseCommand.Body))
				}
				wg.Done()
			})
		if err != nil {
			t.Errorf("failed to invokeSync. %s", err)
		}

	}()

	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := createScanner(conn)
		for scanner.Scan() {
			receivedRemotingCommand, err := decode(scanner.Bytes())
			if err != nil {
				t.Errorf("failed to decode RemotingCommnad. %s", err)
			}
			if clientSendRemtingCommand.Code != receivedRemotingCommand.Code {
				t.Errorf("wrong code. want=%d, got=%d", receivedRemotingCommand.Code,
					clientSendRemtingCommand.Code)
			}
			body, err := encode(serverSendRemotingCommand)
			if err != nil {
				t.Fatalf("failed to encode RemotingCommand")
			}
			_, err = conn.Write(body)
			if err != nil {
				t.Fatalf("failed to write body to conneciton.")
			}
			return
		}
	}
	wg.Done()
}

func TestInvokeOneWay(t *testing.T) {
	clientSendRemtingCommand := NewRemotingCommand(10, nil, []byte("Hello RocketMQ"))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := InvokeOneWay(":3000", clientSendRemtingCommand)
		if err != nil {
			t.Fatalf("failed to invoke synchronous. %s", err)
		}
		wg.Done()
	}()

	l, err := net.Listen("tcp", ":3000")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		scanner := createScanner(conn)
		for scanner.Scan() {
			receivedRemotingCommand, err := decode(scanner.Bytes())
			if err != nil {
				t.Errorf("failed to decode RemotingCommnad. %s", err)
			}
			if clientSendRemtingCommand.Code != receivedRemotingCommand.Code {
				t.Errorf("wrong code. want=%d, got=%d", receivedRemotingCommand.Code,
					clientSendRemtingCommand.Code)
			}
			return
		}
	}
	wg.Done()
}
