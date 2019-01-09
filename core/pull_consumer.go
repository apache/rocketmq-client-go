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
#include <rocketmq/CMessageExt.h>
#include <rocketmq/CPullConsumer.h>
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

// defaultPullConsumer default consumer pulling the message
type defaultPullConsumer struct {
	PullConsumerConfig
	cconsumer *C.struct_CPullConsumer
	funcsMap  sync.Map
}

func (c *defaultPullConsumer) String() string {
	topics := ""
	c.funcsMap.Range(func(key, value interface{}) bool {
		topics += key.(string) + ", "
		return true
	})
	return fmt.Sprintf("[%+v, subcribed topics: [%s]]", c.PullConsumerConfig, topics)
}

// NewPullConsumer creates a pull consumer
func NewPullConsumer(config *PullConsumerConfig) (PullConsumer, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}
	if config.GroupID == "" {
		return nil, errors.New("GroupId is empty")
	}

	if config.NameServer == "" && config.NameServerDomain == "" {
		return nil, errors.New("NameServer and NameServerDomain is empty")
	}

	cs := C.CString(config.GroupID)
	cconsumer := C.CreatePullConsumer(cs)
	C.free(unsafe.Pointer(cs))
	if cconsumer == nil {
		return nil, errors.New("create PullConsumer failed")
	}

	var err rmqError
	if config.NameServer != "" {
		cs = C.CString(config.NameServer)
		err = rmqError(C.SetPullConsumerNameServerAddress(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.NameServerDomain != "" {
		cs = C.CString(config.NameServerDomain)
		err = rmqError(C.SetPullConsumerNameServerDomain(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.Credentials != nil {
		ak := C.CString(config.Credentials.AccessKey)
		sk := C.CString(config.Credentials.SecretKey)
		ch := C.CString(config.Credentials.Channel)
		err = rmqError(C.SetPullConsumerSessionCredentials(cconsumer, ak, sk, ch))
		C.free(unsafe.Pointer(ak))
		C.free(unsafe.Pointer(sk))
		C.free(unsafe.Pointer(ch))
		if err != NIL {
			return nil, err
		}
	}

	if config.LogC != nil {
		cs = C.CString(config.LogC.Path)
		err = rmqError(C.SetPullConsumerLogPath(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}

		err = rmqError(C.SetPullConsumerLogFileNumAndSize(cconsumer, C.int(config.LogC.FileNum), C.long(config.LogC.FileSize)))
		if err != NIL {
			return nil, err
		}

		err = rmqError(C.SetPullConsumerLogLevel(cconsumer, C.CLogLevel(config.LogC.Level)))
		if err != NIL {
			return nil, err
		}
	}

	return &defaultPullConsumer{PullConsumerConfig: *config, cconsumer: cconsumer}, nil
}

// Start starts the pulling consumer
func (c *defaultPullConsumer) Start() error {
	err := rmqError(C.StartPullConsumer(c.cconsumer))
	if err != NIL {
		return err
	}
	return nil
}

// Shutdown shutdown the pulling consumer
func (c *defaultPullConsumer) Shutdown() error {
	err := rmqError(C.ShutdownPullConsumer(c.cconsumer))
	if err != NIL {
		return err
	}

	err = rmqError(C.DestroyPullConsumer(c.cconsumer))
	if err != NIL {
		return err
	}
	return nil
}

// FetchSubscriptionMessageQueues returns the topic's consume queue
func (c *defaultPullConsumer) FetchSubscriptionMessageQueues(topic string) []MessageQueue {
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
func (c *defaultPullConsumer) Pull(mq MessageQueue, subExpression string, offset int64, maxNums int) PullResult {
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
