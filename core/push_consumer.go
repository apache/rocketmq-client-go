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
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"
	"sync"
	"unsafe"
)

type MessageModel C.CMessageModel

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
	if config.GroupID == "" {
		return nil, errors.New("GroupId is empty.")
	}

	if config.NameServer == "" && config.NameServerDomain == "" {
		return nil, errors.New("NameServer and NameServerDomain is empty.")
	}

	//if config.Model == nil {
	//	return nil, errors.New("MessageModel is nil")
	//}

	consumer := &defaultPushConsumer{config: config}
	cconsumer := C.CreatePushConsumer(C.CString(config.GroupID))

	if cconsumer == nil {
		log.Fatal("Create PushConsumer failed, please check cpp logs for details.")
	}

	var code int
	if config.NameServer != "" {
		code = int(C.SetPushConsumerNameServerAddress(cconsumer, C.CString(config.NameServer)))
		if code != 0 {
			 log.Fatalf("PushConsumer Set NameServerAddress error, code is: %d, " +
			 	"please check cpp logs for details", code)
		}
	}

	if config.NameServerDomain != "" {
		code = int(C.SetPushConsumerNameServerDomain(cconsumer, C.CString(config.NameServerDomain)))
		if code != 0 {
			log.Fatalf("PushConsumer Set NameServerDomain error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.InstanceName != "" {
		code = int(C.SetPushConsumerInstanceName(cconsumer, C.CString(config.InstanceName)))
		if code != 0 {
			log.Fatalf("PushConsumer Set InstanceName error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.Credentials != nil {
		code = int(C.SetPushConsumerSessionCredentials(cconsumer,
			C.CString(config.Credentials.AccessKey),
			C.CString(config.Credentials.SecretKey),
			C.CString(config.Credentials.Channel)))
		if code != 0 {
			log.Fatalf("PushConsumer Set Credentials error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.ThreadCount > 0 {
		code = int(C.SetPushConsumerThreadCount(cconsumer, C.int(config.ThreadCount)))
		if code != 0 {
			log.Fatalf("PushConsumer Set ThreadCount error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.MessageBatchMaxSize > 0 {
		code = int(C.SetPushConsumerMessageBatchMaxSize(cconsumer, C.int(config.MessageBatchMaxSize)))
		if code != 0 {
			log.Fatalf("PushConsumer Set MessageBatchMaxSize error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	code = int(C.SetPushConsumerMessageModel(cconsumer, (C.CMessageModel)(config.Model)))

	if code != 0 {
		log.Fatalf("PushConsumer Set ConsumerMessageModel error, code is: %d, " +
			"please check cpp logs for details", code)
	}

	code = int(C.RegisterMessageCallback(cconsumer, (C.MessageCallBack)(unsafe.Pointer(C.callback_cgo))))

	if code != 0 {
		log.Fatalf("PushConsumer RegisterMessageCallback error, code is: %d, " +
			"please check cpp logs for details", code)
	}

	consumer.cconsumer = cconsumer
	pushConsumerMap.Store(cconsumer, consumer)
	return consumer, nil
}

func (c *defaultPushConsumer) Start() error {
	code := C.StartPushConsumer(c.cconsumer)
	if code != 0{
		log.Fatalf("start PushConsumer error, code is: %d, please check cpp logs for details", code)
	}
	return nil
}

func (c *defaultPushConsumer) Shutdown() error {
	code := C.ShutdownPushConsumer(c.cconsumer)

	if code != 0 {
		log.Warnf("Shutdown PushConsumer error, code is: %d, please check cpp logs for details", code)
	}

	C.DestroyPushConsumer(c.cconsumer)
	if code != 0 {
		log.Warnf("Destroy PushConsumer error, code is: %d, please check cpp logs for details", code)
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
		return errors.New(fmt.Sprintf("subscribe topic: %s failed, error code is: %d", topic, int(code)))
	}
	c.funcsMap.Store(topic, consumeFunc)
	log.Infof("subscribe topic[%s] with expression[%s] successfully.", topic, expression)
	return nil
}
