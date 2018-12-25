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
#include <stdlib.h>
#include "rocketmq/CMessageExt.h"
#include "rocketmq/CPushConsumer.h"

extern int consumeMessageCallback(CPushConsumer *consumer, CMessageExt *msg);

int callback_cgo(CPushConsumer *consumer, CMessageExt *msg) {
	return consumeMessageCallback(consumer, msg);
}
*/
import "C"

import (
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"sync"
	"unsafe"
)

type ConsumeStatus int

const (
	ConsumeSuccess = ConsumeStatus(C.E_CONSUME_SUCCESS)
	ReConsumeLater = ConsumeStatus(C.E_RECONSUME_LATER)
)

func (status ConsumeStatus) String() string {
	switch status {
	case ConsumeSuccess:
		return "ConsumeSuccess"
	case ReConsumeLater:
		return "ReConsumeLater"
	default:
		return "Unknown"
	}
}

type defaultPushConsumer struct {
	config    *PushConsumerConfig
	cconsumer *C.struct_CPushConsumer
	funcsMap  sync.Map
}

func (c *defaultPushConsumer) String() string {
	topics := ""
	c.funcsMap.Range(func(key, value interface{}) bool {
		topics += key.(string) + ", "
		return true
	})
	return fmt.Sprintf("[%s, subcribed topics: [%s]]", c.config, topics)
}

func newPushConsumer(config *PushConsumerConfig) (PushConsumer, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}
	if config.GroupID == "" {
		return nil, errors.New("GroupId is empty.")
	}

	if config.NameServer == "" && config.NameServerDomain == "" {
		return nil, errors.New("NameServer and NameServerDomain is empty.")
	}

	consumer := &defaultPushConsumer{config: config}
	cs := C.CString(config.GroupID)
	cconsumer := C.CreatePushConsumer(cs)
	C.free(unsafe.Pointer(cs))

	if cconsumer == nil {
		return nil, errors.New("Create PushConsumer failed")
	}

	var code int
	if config.NameServer != "" {
		cs = C.CString(config.NameServer)
		code = int(C.SetPushConsumerNameServerAddress(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set NameServerAddress error, code is: %d", code)
		}
	}

	if config.NameServerDomain != "" {
		cs = C.CString(config.NameServerDomain)
		code = int(C.SetPushConsumerNameServerDomain(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set NameServerDomain error, code is: %d", code)
		}
	}

	if config.InstanceName != "" {
		cs = C.CString(config.InstanceName)
		code = int(C.SetPushConsumerInstanceName(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set InstanceName error, code is: %d", code)
		}
	}

	if config.Credentials != nil {
		ak := C.CString(config.Credentials.AccessKey)
		sk := C.CString(config.Credentials.SecretKey)
		ch := C.CString(config.Credentials.Channel)
		code = int(C.SetPushConsumerSessionCredentials(cconsumer, ak, sk, ch))
		C.free(unsafe.Pointer(ak))
		C.free(unsafe.Pointer(sk))
		C.free(unsafe.Pointer(ch))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set Credentials error, code is: %d", int(code))
		}
	}

	if config.LogC != nil {
		cs = C.CString(config.LogC.Path)
		code = int(C.SetPushConsumerLogPath(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set LogPath error, code is: %d", code)
		}

		code = int(C.SetPushConsumerLogFileNumAndSize(cconsumer, C.int(config.LogC.FileNum), C.long(config.LogC.FileSize)))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set FileNumAndSize error, code is: %d", code)
		}

		code = int(C.SetPushConsumerLogLevel(cconsumer, C.CLogLevel(config.LogC.Level)))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set LogLevel error, code is: %d", code)
		}
	}

	if config.ThreadCount > 0 {
		code = int(C.SetPushConsumerThreadCount(cconsumer, C.int(config.ThreadCount)))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set ThreadCount error, code is: %d", int(code))
		}
	}

	if config.MessageBatchMaxSize > 0 {
		code = int(C.SetPushConsumerMessageBatchMaxSize(cconsumer, C.int(config.MessageBatchMaxSize)))
		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set MessageBatchMaxSize error, code is: %d", int(code))
		}
	}

	if config.Model != 0 {
		var mode C.CMessageModel
		switch config.Model {
		case BroadCasting:
			mode = C.BROADCASTING
		case Clustering:
			mode = C.CLUSTERING
		}
		code = int(C.SetPushConsumerMessageModel(cconsumer, mode))

		if code != 0 {
			return nil, fmt.Errorf("PushConsumer Set ConsumerMessageModel error, code is: %d", int(code))
		}

	}

	code = int(C.RegisterMessageCallback(cconsumer, (C.MessageCallBack)(unsafe.Pointer(C.callback_cgo))))

	if code != 0 {
		return nil, fmt.Errorf("PushConsumer RegisterMessageCallback error, code is: %d", int(code))
	}

	consumer.cconsumer = cconsumer
	pushConsumerMap.Store(cconsumer, consumer)
	return consumer, nil
}

func (c *defaultPushConsumer) Start() error {
	code := C.StartPushConsumer(c.cconsumer)
	if code != 0 {
		return fmt.Errorf("start PushConsumer error, code is: %d", int(code))
	}
	return nil
}

func (c *defaultPushConsumer) Shutdown() error {
	code := C.ShutdownPushConsumer(c.cconsumer)

	if code != 0 {
		log.Warnf("Shutdown PushConsumer error, code is: %d", code)
	}

	C.DestroyPushConsumer(c.cconsumer)
	if code != 0 {
		log.Warnf("Destroy PushConsumer error, code is: %d", code)
	}
	return nil
}

func (c *defaultPushConsumer) Subscribe(topic, expression string, consumeFunc func(msg *MessageExt) ConsumeStatus) error {
	_, exist := c.funcsMap.Load(topic)
	if exist {
		return nil
	}
	code := C.Subscribe(c.cconsumer, C.CString(topic), C.CString(expression))
	if code != 0 {
		return fmt.Errorf("subscribe topic: %s failed, error code is: %d", topic, int(code))
	}
	c.funcsMap.Store(topic, consumeFunc)
	log.Infof("subscribe topic[%s] with expression[%s] successfully.", topic, expression)
	return nil
}
