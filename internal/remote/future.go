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
)

// ResponseFuture
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

// NewResponseFuture create ResponseFuture with opaque, timeout and callback
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
	var (
		cmd *RemotingCommand
		err error
	)
	timer := time.NewTimer(r.TimeoutMillis * time.Millisecond)
	for {
		select {
		case <-r.Done:
			cmd, err = r.ResponseCommand, r.Err
			goto done
		case <-timer.C:
			err = ErrRequestTimeout
			r.Err = err
			goto done
		}
	}
done:
	timer.Stop()
	return cmd, err
}
