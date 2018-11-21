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
#include "rocketmq/CMessageExt.h"
#include "rocketmq/CPushConsumer.h"
#include "stdio.h"

extern int consumeMessageCallback(CPushConsumer *consumer, CMessageExt *msg);

int callback_cgo(CPushConsumer *consumer, CMessageExt *msg) {
	return consumeMessageCallback(consumer, msg);
}
*/
import "C"

import (
	"fmt"
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
	config    *ConsumerConfig
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

func newPushConsumer(config *ConsumerConfig) (PushConsumer, error) {
	consumer := &defaultPushConsumer{config: config}
	cconsumer := C.CreatePushConsumer(C.CString(config.GroupID))
	C.SetPushConsumerNameServerAddress(cconsumer, C.CString(config.NameServer))
	C.SetPushConsumerThreadCount(cconsumer, C.int(config.ConsumerThreadCount))
	C.SetPushConsumerMessageBatchMaxSize(cconsumer, C.int(config.ConsumerThreadCount))
	C.RegisterMessageCallback(cconsumer, (C.MessageCallBack)(unsafe.Pointer(C.callback_cgo)))
	if config.Credentials != nil {
		C.SetPushConsumerSessionCredentials(cconsumer,
			C.CString(config.Credentials.AccessKey),
			C.CString(config.Credentials.SecretKey),
			C.CString(config.Credentials.Channel))
	}

	consumer.cconsumer = cconsumer
	pushConsumerMap.Store(cconsumer, consumer)
	return consumer, nil
}

func (c *defaultPushConsumer) Start() error {
	C.StartPushConsumer(c.cconsumer)
	return nil
}

func (c *defaultPushConsumer) Shutdown() error {
	C.ShutdownPushConsumer(c.cconsumer)
	C.DestroyPushConsumer(c.cconsumer)
	return nil
}

func (c *defaultPushConsumer) Subscribe(topic, expression string, consumeFunc func(msg *MessageExt) ConsumeStatus) error {
	_, exist := c.funcsMap.Load(topic)
	if exist {
		return nil
	}
	err := C.Subscribe(c.cconsumer, C.CString(topic), C.CString(expression))
	fmt.Println("err:", err)
	c.funcsMap.Store(topic, consumeFunc)
	fmt.Printf("subscribe topic[%s] with expression[%s] successfully. \n", topic, expression)
	return nil
}
