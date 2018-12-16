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

package rocketmq

/*
#cgo LDFLAGS: -L/usr/local/lib -lrocketmq

#include <stdio.h>
#include <stdlib.h>
#include "rocketmq/CMessageExt.h"
#include "rocketmq/CPullConsumer.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// PullStatus pull status
type PullStatus int

// predefined pull status
const (
	PullFound         = PullStatus(C.E_FOUND)
	PullNoNewMsg      = PullStatus(C.E_NO_NEW_MSG)
	PullNoMatchedMsg  = PullStatus(C.E_NO_MATCHED_MSG)
	PullOffsetIllegal = PullStatus(C.E_OFFSET_ILLEGAL)
	PullBrokerTimeout = PullStatus(C.E_BROKER_TIMEOUT)
)

func (ps PullStatus) String() string {
	switch ps {
	case PullFound:
		return "Found"
	case PullNoNewMsg:
		return "NoNewMsg"
	case PullNoMatchedMsg:
		return "NoMatchedMsg"
	case PullOffsetIllegal:
		return "OffsetIllegal"
	case PullBrokerTimeout:
		return "BrokerTimeout"
	default:
		return "Unknown status"
	}
}

// PullConsumerConfig the configuration for the pull consumer
type PullConsumerConfig struct {
	clientConfig
}

// DefaultPullConsumer default consumer pulling the message
type DefaultPullConsumer struct {
	PullConsumerConfig
	cconsumer *C.struct_CPullConsumer
	funcsMap  sync.Map
}

func (c *DefaultPullConsumer) String() string {
	topics := ""
	c.funcsMap.Range(func(key, value interface{}) bool {
		topics += key.(string) + ", "
		return true
	})
	return fmt.Sprintf("[%+v, subcribed topics: [%s]]", c.PullConsumerConfig, topics)
}

// NewPullConsumer creates one pull consumer
func NewPullConsumer(conf *PullConsumerConfig) (*DefaultPullConsumer, error) {
	if conf == nil {
		return nil, errors.New("config is nil")
	}

	cs := C.CString(conf.GroupID)
	cconsumer := C.CreatePullConsumer(cs)
	C.free(unsafe.Pointer(cs))

	cs = C.CString(conf.NameServer)
	C.SetPullConsumerNameServerAddress(cconsumer, cs)
	C.free(unsafe.Pointer(cs))

	log := conf.LogC
	if log != nil {
		cs = C.CString(log.Path)
		if C.SetPullConsumerLogPath(cconsumer, cs) != 0 {
			return nil, errors.New("new pull consumer error:set log path failed")
		}
		C.free(unsafe.Pointer(cs))

		if C.SetPullConsumerLogFileNumAndSize(cconsumer, C.int(log.FileNum), C.long(log.FileSize)) != 0 {
			return nil, errors.New("new pull consumer error:set log file num and size failed")
		}
		if C.SetPullConsumerLogLevel(cconsumer, C.CLogLevel(log.Level)) != 0 {
			return nil, errors.New("new pull consumer error:set log level failed")
		}
	}

	if conf.Credentials != nil {
		ak := C.CString(conf.Credentials.AccessKey)
		sk := C.CString(conf.Credentials.SecretKey)
		ch := C.CString(conf.Credentials.Channel)
		C.SetPullConsumerSessionCredentials(cconsumer, ak, sk, ch)

		C.free(unsafe.Pointer(ak))
		C.free(unsafe.Pointer(sk))
		C.free(unsafe.Pointer(ch))
	}

	return &DefaultPullConsumer{PullConsumerConfig: *conf, cconsumer: cconsumer}, nil
}

// Start starts the pulling conumser
func (c *DefaultPullConsumer) Start() error {
	r := C.StartPullConsumer(c.cconsumer)
	if r != 0 {
		return fmt.Errorf("start failed, code:%d", int(r))
	}
	return nil
}

// Shutdown shutdown the pulling consumer
func (c *DefaultPullConsumer) Shutdown() error {
	r := C.ShutdownPullConsumer(c.cconsumer)
	if r != 0 {
		return fmt.Errorf("shutdown failed, code:%d", int(r))
	}

	r = C.DestroyPullConsumer(c.cconsumer)
	if r != 0 {
		return fmt.Errorf("destory failed, code:%d", int(r))
	}
	return nil
}

// FetchSubscriptionMessageQueues returns the topic's consume queue
func (c *DefaultPullConsumer) FetchSubscriptionMessageQueues(topic string) []MessageQueue {
	var (
		q    *C.struct__CMessageQueue_
		size C.int
	)

	ctopic := C.CString(topic)
	C.FetchSubscriptionMessageQueues(c.cconsumer, ctopic, &q, &size)
	C.free(unsafe.Pointer(ctopic))
	if size == 0 {
		return nil
	}

	qs := make([]MessageQueue, size)
	for i := range qs {
		cq := (*C.struct__CMessageQueue_)(
			unsafe.Pointer(uintptr(unsafe.Pointer(q)) + uintptr(i)*unsafe.Sizeof(*q)),
		)
		qs[i].ID, qs[i].Broker, qs[i].Topic = int(cq.queueId), C.GoString(&cq.brokerName[0]), topic
	}
	C.ReleaseSubscriptionMessageQueue(q)

	return qs
}

// PullResult the pull result
type PullResult struct {
	NextBeginOffset int64
	MinOffset       int64
	MaxOffset       int64
	Status          PullStatus
	Messages        []*MessageExt
}

func (pr *PullResult) String() string {
	return fmt.Sprintf("%+v", *pr)
}

// Pull pulling the message from the specified message queue
func (c *DefaultPullConsumer) Pull(mq MessageQueue, subExpression string, offset int64, maxNums int) PullResult {
	cmq := C.struct__CMessageQueue_{
		queueId: C.int(mq.ID),
	}

	copy(cmq.topic[:], *(*[]C.char)(unsafe.Pointer(&mq.Topic)))
	copy(cmq.brokerName[:], *(*[]C.char)(unsafe.Pointer(&mq.Broker)))

	csubExpr := C.CString(subExpression)
	cpullResult := C.Pull(c.cconsumer, &cmq, csubExpr, C.longlong(offset), C.int(maxNums))

	pr := PullResult{
		NextBeginOffset: int64(cpullResult.nextBeginOffset),
		MinOffset:       int64(cpullResult.minOffset),
		MaxOffset:       int64(cpullResult.maxOffset),
		Status:          PullStatus(cpullResult.pullStatus),
	}
	if cpullResult.size > 0 {
		msgs := make([]*MessageExt, cpullResult.size)
		for i := range msgs {
			msgs[i] = cmsgExtToGo(*(**C.struct_CMessageExt)(
				unsafe.Pointer(
					uintptr(unsafe.Pointer(cpullResult.msgFoundList)) + uintptr(i)*unsafe.Sizeof(*cpullResult.msgFoundList),
				),
			))
		}
		pr.Messages = msgs
	}

	C.free(unsafe.Pointer(csubExpr))
	C.ReleasePullResult(cpullResult)
	return pr
}
