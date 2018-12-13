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
#cgo LDFLAGS: -L/usr/local/lib/ -lrocketmq

#include <stdio.h>
#include "rocketmq/CMessage.h"
#include "rocketmq/CProducer.h"
#include "rocketmq/CSendResult.h"

int queueSelectorCallback_cgo(int size, CMessage *msg, void *selectorKey) {
	int queueSelectorCallback(int, void*);
	return queueSelectorCallback(size, selectorKey);
}
*/
import "C"
import (
	"errors"
	log "github.com/sirupsen/logrus"
	"unsafe"
)

type SendStatus int

const (
	SendOK                = SendStatus(C.E_SEND_OK)
	SendFlushDiskTimeout  = SendStatus(C.E_SEND_FLUSH_DISK_TIMEOUT)
	SendFlushSlaveTimeout = SendStatus(C.E_SEND_FLUSH_SLAVE_TIMEOUT)
	SendSlaveNotAvailable = SendStatus(C.E_SEND_SLAVE_NOT_AVAILABLE)
)

func (status SendStatus) String() string {
	switch status {
	case SendOK:
		return "SendOK"
	case SendFlushDiskTimeout:
		return "SendFlushDiskTimeout"
	case SendFlushSlaveTimeout:
		return "SendFlushSlaveTimeout"
	case SendSlaveNotAvailable:
		return "SendSlaveNotAvailable"
	default:
		return "Unknown"
	}
}

func newDefaultProducer(config *ProducerConfig) (*defaultProducer, error) {
	if config.GroupID == "" {
		return nil, errors.New("GroupId is empty.")
	}

	if config.NameServer == "" && config.NameServerDomain == "" {
		return nil, errors.New("NameServer and NameServerDomain is empty.")
	}


	producer := &defaultProducer{config: config}
	cproduer := C.CreateProducer(C.CString(config.GroupID))
	
	if cproduer == nil {
		log.Fatal("Create Producer failed, please check cpp logs for details.")
	}

	var code int
	if config.NameServer != "" {
		code = int(C.SetProducerNameServerAddress(cproduer, C.CString(config.NameServer)))
		if code != 0 {
			log.Fatalf("Producer Set NameServerAddress error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.NameServerDomain != "" {
		code = int(C.SetProducerNameServerDomain(cproduer, C.CString(config.NameServerDomain)))
		if code != 0 {
			log.Fatalf("Producer Set NameServerDomain error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.InstanceName != "" {
		code = int(C.SetProducerInstanceName(cproduer, C.CString(config.InstanceName)))
		if code != 0 {
			log.Fatalf("Producer Set InstanceName error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.Credentials != nil {
		code = int(C.SetProducerSessionCredentials(cproduer,
			C.CString(config.Credentials.AccessKey),
			C.CString(config.Credentials.SecretKey),
			C.CString(config.Credentials.Channel)))
		if code != 0 {
			log.Fatalf("Producer Set Credentials error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.SendMsgTimeout > 0 {
		code = int(C.SetProducerSendMsgTimeout(cproduer, C.int(config.SendMsgTimeout)))
		if code != 0 {
			log.Fatalf("Producer Set SendMsgTimeout error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.CompressLevel > 0 {
		code = int(C.SetProducerCompressLevel(cproduer, C.int(config.CompressLevel)))
		if code != 0 {
			log.Fatalf("Producer Set CompressLevel error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	if config.MaxMessageSize > 0 {
		code = int(C.SetProducerMaxMessageSize(cproduer, C.int(config.MaxMessageSize)))
		if code != 0 {
			log.Fatalf("Producer Set MaxMessageSize error, code is: %d, " +
				"please check cpp logs for details", code)
		}
	}

	producer.cproduer = cproduer
	return producer, nil
}

type defaultProducer struct {
	config   *ProducerConfig
	cproduer *C.struct_CProducer
}

func (p *defaultProducer) String() string {
	return p.config.String()
}

// Start the producer.
func (p *defaultProducer) Start() error {
	code := int(C.StartProducer(p.cproduer))
	if code != 0 {
		 log.Fatalf("start producer error, error code is: %d", code)
	}
	return nil
}

// Shutdown the producer.
func (p *defaultProducer) Shutdown() error {
	code := int(C.ShutdownProducer(p.cproduer))

	if code != 0 {
		log.Warnf("shutdown producer error, error code is: %d", code)
	}

	code = int(int(C.DestroyProducer(p.cproduer)))
	if code != 0 {
		log.Warnf("destroy producer error, error code is: %d", code)
	}

	return nil
}

func (p *defaultProducer) SendMessageSync(msg *Message) SendResult {
	cmsg := goMsgToC(msg)
	defer C.DestroyMessage(cmsg)

	var sr C.struct__SendResult_
	code := int(C.SendMessageSync(p.cproduer, cmsg, &sr))

	if code != 0 {
		log.Warnf("send message error, error code is: %d", code)
	}

	result := SendResult{}
	result.Status = SendStatus(sr.sendStatus)
	result.MsgId = C.GoString(&sr.msgId[0])
	result.Offset = int64(sr.offset)
	return result
}

func (p *defaultProducer) SendMessageOrderly(msg *Message, selector MessageQueueSelector, arg interface{}, autoRetryTimes int) SendResult {
	cmsg := goMsgToC(msg)
	key := selectors.put(&messageQueueSelectorWrapper{selector: selector, m: msg, arg: arg})

	var sr C.struct__SendResult_
	C.SendMessageOrderly(
		p.cproduer,
		cmsg,
		(C.QueueSelectorCallback)(unsafe.Pointer(C.queueSelectorCallback_cgo)),
		unsafe.Pointer(&key),
		C.int(autoRetryTimes),
		&sr,
	)
	C.DestroyMessage(cmsg)

	return SendResult{
		Status: SendStatus(sr.sendStatus),
		MsgId:  C.GoString(&sr.msgId[0]),
		Offset: int64(sr.offset),
	}
}

func (p *defaultProducer) SendMessageAsync(msg *Message) {
	// TODO
}
