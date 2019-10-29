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
#include <rocketmq/CMessageExt.h>
#include <rocketmq/CPushConsumer.h>

extern int consumeMessageCallback(CPushConsumer *consumer, CMessageExt *msg);

int callback_cgo(CPushConsumer *consumer, CMessageExt *msg) {
	return consumeMessageCallback(consumer, msg);
}
*/
import "C"

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"unsafe"
)

//ConsumeStatus the retern value for consumer
type ConsumeStatus int

const (
	//ConsumeSuccess commit offset to broker
	ConsumeSuccess = ConsumeStatus(C.E_CONSUME_SUCCESS)
	//ReConsumeLater it will be send back to broker
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
		return nil, errors.New("GroupId is empty")
	}

	if config.NameServer == "" && config.NameServerDomain == "" {
		return nil, errors.New("NameServer and NameServerDomain is empty")
	}

	consumer := &defaultPushConsumer{config: config}
	cs := C.CString(config.GroupID)
	cconsumer := C.CreatePushConsumer(cs)
	C.free(unsafe.Pointer(cs))

	if cconsumer == nil {
		return nil, errors.New("create PushConsumer failed")
	}

	var err rmqError
	if config.NameServer != "" {
		cs = C.CString(config.NameServer)
		err = rmqError(C.SetPushConsumerNameServerAddress(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.NameServerDomain != "" {
		cs = C.CString(config.NameServerDomain)
		err = rmqError(C.SetPushConsumerNameServerDomain(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.InstanceName != "" {
		cs = C.CString(config.InstanceName)
		err = rmqError(C.SetPushConsumerInstanceName(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.Credentials != nil {
		ak := C.CString(config.Credentials.AccessKey)
		sk := C.CString(config.Credentials.SecretKey)
		ch := C.CString(config.Credentials.Channel)
		err = rmqError(C.SetPushConsumerSessionCredentials(cconsumer, ak, sk, ch))
		C.free(unsafe.Pointer(ak))
		C.free(unsafe.Pointer(sk))
		C.free(unsafe.Pointer(ch))
		if err != NIL {
			return nil, err
		}
	}

	if config.LogC != nil {
		cs = C.CString(config.LogC.Path)
		err = rmqError(C.SetPushConsumerLogPath(cconsumer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}

		err = rmqError(C.SetPushConsumerLogFileNumAndSize(cconsumer, C.int(config.LogC.FileNum), C.long(config.LogC.FileSize)))
		if err != NIL {
			return nil, err
		}

		err = rmqError(C.SetPushConsumerLogLevel(cconsumer, C.CLogLevel(config.LogC.Level)))
		if err != NIL {
			return nil, err
		}
	}

	if config.ThreadCount > 0 {
		err = rmqError(C.SetPushConsumerThreadCount(cconsumer, C.int(config.ThreadCount)))
		if err != NIL {
			return nil, err
		}
	}

	if config.MessageBatchMaxSize > 0 {
		err = rmqError(C.SetPushConsumerMessageBatchMaxSize(cconsumer, C.int(config.MessageBatchMaxSize)))
		if err != NIL {
			return nil, err
		}
	}

	if config.MaxCacheMessageSize > 0 {
		err = rmqError(C.SetPushConsumerMaxCacheMessageSize(cconsumer, C.int(config.MaxCacheMessageSize)))
		if err != NIL {
			return nil, err
		}
	}

	if config.MaxCacheMessageSizeInMB > 0 {
		err = rmqError(C.SetPushConsumerMaxCacheMessageSizeInMb(cconsumer, C.int(config.MaxCacheMessageSizeInMB)))
		if err != NIL {
			return nil, err
		}
	}

	if config.Model == BroadCasting {
		err = rmqError(C.SetPushConsumerMessageModel(cconsumer, C.BROADCASTING))
	} else if config.Model == Clustering {
		err = rmqError(C.SetPushConsumerMessageModel(cconsumer, C.CLUSTERING))
	} else {
		return nil, errors.New("model is invalid or empty")
	}
	if err != NIL {
		return nil, err
	}

	if config.ConsumerModel == Orderly {
		err = rmqError(C.RegisterMessageCallbackOrderly(cconsumer, (C.MessageCallBack)(unsafe.Pointer(C.callback_cgo))))
	} else if config.ConsumerModel == CoCurrently {
		err = rmqError(C.RegisterMessageCallback(cconsumer, (C.MessageCallBack)(unsafe.Pointer(C.callback_cgo))))
	} else {
		return nil, errors.New("consumer model is invalid or empty")
	}
	if err != NIL {
		return nil, err
	}

	consumer.cconsumer = cconsumer
	pushConsumerMap.Store(cconsumer, consumer)
	return consumer, nil
}

func (c *defaultPushConsumer) Start() error {
	err := rmqError(C.StartPushConsumer(c.cconsumer))
	if err != NIL {
		return err
	}
	return nil
}

func (c *defaultPushConsumer) Shutdown() error {
	err := rmqError(C.ShutdownPushConsumer(c.cconsumer))

	if err != NIL {
		return err
	}

	err = rmqError(C.DestroyPushConsumer(c.cconsumer))
	if err != NIL {
		return err
	}
	return nil
}

func (c *defaultPushConsumer) Subscribe(topic, expression string, consumeFunc func(msg *MessageExt) ConsumeStatus) error {
	if consumeFunc == nil {
		return errors.New("consumeFunc is nil")
	}
	err := rmqError(C.Subscribe(c.cconsumer, C.CString(topic), C.CString(expression)))
	if err != NIL {
		return err
	}
	c.funcsMap.Store(topic, consumeFunc)
	log.Infof("subscribe topic[%s] with expression[%s] successfully.", topic, expression)
	return nil
}
