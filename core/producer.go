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
#include <stdlib.h>
#include <rocketmq/CMessage.h>
#include <rocketmq/CProducer.h>
#include <rocketmq/CSendResult.h>

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

//SendStatus The Status for send result from C apis.
type SendStatus int

const (
	//SendOK OK
	SendOK = SendStatus(C.E_SEND_OK)
	//SendFlushDiskTimeout Failed because broker flush error
	SendFlushDiskTimeout = SendStatus(C.E_SEND_FLUSH_DISK_TIMEOUT)
	//SendFlushSlaveTimeout Failed because slave broker timeout
	SendFlushSlaveTimeout = SendStatus(C.E_SEND_FLUSH_SLAVE_TIMEOUT)
	//SendSlaveNotAvailable Failed because slave broker error
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
	if config == nil {
		return nil, errors.New("config is nil")
	}

	if config.GroupID == "" {
		return nil, errors.New("GroupId is empty")
	}

	if config.NameServer == "" && config.NameServerDomain == "" {
		return nil, errors.New("NameServer and NameServerDomain is empty")
	}

	producer := &defaultProducer{config: config}
	cs := C.CString(config.GroupID)
	var cproducer *C.struct_CProducer
	if config.ProducerModel == OrderlyProducer {
		cproducer = C.CreateOrderlyProducer(cs)
	} else if config.ProducerModel == CommonProducer {
		cproducer = C.CreateProducer(cs)
	} else {
		C.free(unsafe.Pointer(cs))
		return nil, errors.New("ProducerModel is invalid or empty")
	}
	C.free(unsafe.Pointer(cs))

	if cproducer == nil {
		return nil, errors.New("create Producer failed")
	}

	var err rmqError
	if config.NameServer != "" {
		cs = C.CString(config.NameServer)
		err = rmqError(C.SetProducerNameServerAddress(cproducer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.NameServerDomain != "" {
		cs = C.CString(config.NameServerDomain)
		err = rmqError(C.SetProducerNameServerDomain(cproducer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.InstanceName != "" {
		cs = C.CString(config.InstanceName)
		err = rmqError(C.SetProducerInstanceName(cproducer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.Credentials != nil {
		ak := C.CString(config.Credentials.AccessKey)
		sk := C.CString(config.Credentials.SecretKey)
		ch := C.CString(config.Credentials.Channel)
		err = rmqError(C.SetProducerSessionCredentials(cproducer, ak, sk, ch))

		C.free(unsafe.Pointer(ak))
		C.free(unsafe.Pointer(sk))
		C.free(unsafe.Pointer(ch))
		if err != NIL {
			return nil, err
		}
	}

	if config.LogC != nil {
		cs = C.CString(config.LogC.Path)
		err = rmqError(C.SetProducerLogPath(cproducer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}

		err = rmqError(C.SetProducerLogFileNumAndSize(cproducer, C.int(config.LogC.FileNum), C.long(config.LogC.FileSize)))
		if err != NIL {
			return nil, err
		}

		err = rmqError(C.SetProducerLogLevel(cproducer, C.CLogLevel(config.LogC.Level)))
		if err != NIL {
			return nil, err
		}
	}

	if config.SendMsgTimeout > 0 {
		err = rmqError(C.SetProducerSendMsgTimeout(cproducer, C.int(config.SendMsgTimeout)))
		if err != NIL {
			return nil, err
		}
	}

	if config.CompressLevel > 0 {
		err = rmqError(C.SetProducerCompressLevel(cproducer, C.int(config.CompressLevel)))
		if err != NIL {
			return nil, err
		}
	}

	if config.MaxMessageSize > 0 {
		err = rmqError(C.SetProducerMaxMessageSize(cproducer, C.int(config.MaxMessageSize)))
		if err != NIL {
			return nil, err
		}
	}

	producer.cproducer = cproducer
	return producer, nil
}

type defaultProducer struct {
	config   *ProducerConfig
	cproducer *C.struct_CProducer
}

func (p *defaultProducer) String() string {
	return p.config.String()
}

// Start the producer.
func (p *defaultProducer) Start() error {
	err := rmqError(C.StartProducer(p.cproducer))
	if err != NIL {
		return err
	}
	return nil
}

// Shutdown the producer.
func (p *defaultProducer) Shutdown() error {
	err := rmqError(C.ShutdownProducer(p.cproducer))

	if err != NIL {
		return err
	}

	err = rmqError(int(C.DestroyProducer(p.cproducer)))
	if err != NIL {
		return err
	}

	return err
}

func (p *defaultProducer) SendMessageSync(msg *Message) (*SendResult, error) {
	cmsg := goMsgToC(msg)
	defer C.DestroyMessage(cmsg)

	var sr C.struct__SendResult_
	err := rmqError(C.SendMessageSync(p.cproducer, cmsg, &sr))

	if err != NIL {
		log.Warnf("send message error, error is: %s", err.Error())
		return nil, err
	}

	result := &SendResult{}
	result.Status = SendStatus(sr.sendStatus)
	result.MsgId = C.GoString(&sr.msgId[0])
	result.Offset = int64(sr.offset)
	return result, nil
}

func (p *defaultProducer) SendMessageOrderly(msg *Message, selector MessageQueueSelector, arg interface{}, autoRetryTimes int) (*SendResult, error) {
	if p.config.ProducerModel == OrderlyProducer {
		log.Warnf("Can not send message orderly by common select queue in lite order producer")
		return nil, ErrSendOrderlyFailed
	}
	cmsg := goMsgToC(msg)
	defer C.DestroyMessage(cmsg)
	key := selectors.put(&messageQueueSelectorWrapper{selector: selector, m: msg, arg: arg})

	var sr C.struct__SendResult_
	err := rmqError(C.SendMessageOrderly(
		p.cproducer,
		cmsg,
		(C.QueueSelectorCallback)(unsafe.Pointer(C.queueSelectorCallback_cgo)),
		unsafe.Pointer(&key),
		C.int(autoRetryTimes),
		&sr))

	if err != NIL {
		log.Warnf("send message orderly error, error is: %s", err.Error())
		return nil, err
	}

	return &SendResult{
		Status: SendStatus(sr.sendStatus),
		MsgId:  C.GoString(&sr.msgId[0]),
		Offset: int64(sr.offset),
	}, nil
}

func (p *defaultProducer) SendMessageOneway(msg *Message) error {
	cmsg := goMsgToC(msg)
	defer C.DestroyMessage(cmsg)

	err := rmqError(C.SendMessageOneway(p.cproducer, cmsg))
	if err != NIL {
		log.Warnf("send message with oneway error, error is: %s", err.Error())
		return err
	}

	log.Debugf("Send Message: %s with oneway success.", msg.String())
	return nil
}

func (p *defaultProducer) SendMessageOrderlyByShardingKey(msg *Message, shardingkey string) (*SendResult, error) {
	if p.config.ProducerModel != OrderlyProducer {
		log.Warnf("Can not send message orderly, This method only support in lite order producer.")
		return nil, ErrSendOrderlyFailed
	}
	cmsg := goMsgToC(msg)
	defer C.DestroyMessage(cmsg)
	cshardingkey := C.CString(shardingkey)
	defer C.free(unsafe.Pointer(cshardingkey))
	var sr C.struct__SendResult_
	err := rmqError(C.SendMessageOrderlyByShardingKey(
		p.cproducer,
		cmsg,
		cshardingkey,
		&sr))

	if err != NIL {
		log.Warnf("send message orderly error, error is: %s", err.Error())
		return nil, err
	}

	return &SendResult{
		Status: SendStatus(sr.sendStatus),
		MsgId:  C.GoString(&sr.msgId[0]),
		Offset: int64(sr.offset),
	}, nil
}
