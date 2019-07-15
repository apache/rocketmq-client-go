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
	"sync"
	"time"

	"github.com/apache/rocketmq-client-go/internal/utils"
)

// ResponseFuture
type ResponseFuture struct {
	ResponseCommand *RemotingCommand
	SendRequestOK   bool
	Err             error
	Opaque          int32
	Timeout         time.Duration
	callback        func(*ResponseFuture)
	BeginTimestamp  time.Duration
	Done            chan bool
	callbackOnce    sync.Once
}

// NewResponseFuture create ResponseFuture with opaque, timeout and callback
func NewResponseFuture(opaque int32, timeout time.Duration, callback func(*ResponseFuture)) *ResponseFuture {
	return &ResponseFuture{
		Opaque:         opaque,
		Done:           make(chan bool),
		Timeout:        timeout,
		callback:       callback,
		BeginTimestamp: time.Duration(time.Now().Unix()) * time.Second,
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
	elapse := time.Duration(time.Now().Unix())*time.Second - r.BeginTimestamp
	return elapse > r.Timeout
}

func (r *ResponseFuture) waitResponse() (*RemotingCommand, error) {
	var (
		cmd *RemotingCommand
		err error
	)
	timer := time.NewTimer(r.Timeout)
	for {
		select {
		case <-r.Done:
			cmd, err = r.ResponseCommand, r.Err
			goto done
		case <-timer.C:
			err = utils.ErrRequestTimeout
			r.Err = err
			goto done
		}
	}
done:
	timer.Stop()
	return cmd, err
}
