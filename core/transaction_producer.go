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
#include <rocketmq/CTransactionStatus.h>

extern int localTransactionCheckerCallback(CProducer *producer, CMessageExt *msg,void *userData);
int transactionChecker_cgo(CProducer *producer, CMessageExt *msg, void *userData) {
	return localTransactionCheckerCallback(producer, msg, userData);
}

extern int localTransactionExecutorCallback(CProducer *producer, CMessage *msg,void *userData);
int transactionExecutor_cgo(CProducer *producer, CMessage *msg, void *userData) {
	return localTransactionExecutorCallback(producer, msg, userData);
}
*/
import "C"
import (
	"errors"
	log "github.com/sirupsen/logrus"
	"sync"
	"unsafe"
)

//TransactionStatus check the status if commit or rollback
type TransactionStatus int

//TransactionStatus check the status if commit or rollback
const (
	CommitTransaction   = TransactionStatus(C.E_COMMIT_TRANSACTION)
	RollbackTransaction = TransactionStatus(C.E_ROLLBACK_TRANSACTION)
	UnknownTransaction  = TransactionStatus(C.E_UNKNOWN_TRANSACTION)
)

func (status TransactionStatus) String() string {
	switch status {
	case CommitTransaction:
		return "CommitTransaction"
	case RollbackTransaction:
		return "RollbackTransaction"
	case UnknownTransaction:
		return "UnknownTransaction"
	default:
		return "UnknownTransaction"
	}
}
func newDefaultTransactionProducer(config *ProducerConfig, listener TransactionLocalListener, arg interface{}) (*defaultTransactionProducer, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}

	if config.GroupID == "" {
		return nil, errors.New("GroupId is empty")
	}

	if config.NameServer == "" && config.NameServerDomain == "" {
		return nil, errors.New("NameServer and NameServerDomain is empty")
	}

	producer := &defaultTransactionProducer{config: config}
	cs := C.CString(config.GroupID)
	var cproduer *C.struct_CProducer

	cproduer = C.CreateTransactionProducer(cs, (C.CLocalTransactionCheckerCallback)(unsafe.Pointer(C.transactionChecker_cgo)), unsafe.Pointer(&arg))

	C.free(unsafe.Pointer(cs))

	if cproduer == nil {
		return nil, errors.New("create transaction Producer failed")
	}

	var err rmqError
	if config.NameServer != "" {
		cs = C.CString(config.NameServer)
		err = rmqError(C.SetProducerNameServerAddress(cproduer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.NameServerDomain != "" {
		cs = C.CString(config.NameServerDomain)
		err = rmqError(C.SetProducerNameServerDomain(cproduer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.InstanceName != "" {
		cs = C.CString(config.InstanceName)
		err = rmqError(C.SetProducerInstanceName(cproduer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}
	}

	if config.Credentials != nil {
		ak := C.CString(config.Credentials.AccessKey)
		sk := C.CString(config.Credentials.SecretKey)
		ch := C.CString(config.Credentials.Channel)
		err = rmqError(C.SetProducerSessionCredentials(cproduer, ak, sk, ch))

		C.free(unsafe.Pointer(ak))
		C.free(unsafe.Pointer(sk))
		C.free(unsafe.Pointer(ch))
		if err != NIL {
			return nil, err
		}
	}

	if config.LogC != nil {
		cs = C.CString(config.LogC.Path)
		err = rmqError(C.SetProducerLogPath(cproduer, cs))
		C.free(unsafe.Pointer(cs))
		if err != NIL {
			return nil, err
		}

		err = rmqError(C.SetProducerLogFileNumAndSize(cproduer, C.int(config.LogC.FileNum), C.long(config.LogC.FileSize)))
		if err != NIL {
			return nil, err
		}

		err = rmqError(C.SetProducerLogLevel(cproduer, C.CLogLevel(config.LogC.Level)))
		if err != NIL {
			return nil, err
		}
	}

	if config.SendMsgTimeout > 0 {
		err = rmqError(C.SetProducerSendMsgTimeout(cproduer, C.int(config.SendMsgTimeout)))
		if err != NIL {
			return nil, err
		}
	}

	if config.CompressLevel > 0 {
		err = rmqError(C.SetProducerCompressLevel(cproduer, C.int(config.CompressLevel)))
		if err != NIL {
			return nil, err
		}
	}

	if config.MaxMessageSize > 0 {
		err = rmqError(C.SetProducerMaxMessageSize(cproduer, C.int(config.MaxMessageSize)))
		if err != NIL {
			return nil, err
		}
	}

	producer.cproduer = cproduer
	transactionProducerMap.Store(cproduer, producer)
	producer.listenerFuncsMap.Store(cproduer, listener)
	return producer, nil
}

type defaultTransactionProducer struct {
	config           *ProducerConfig
	cproduer         *C.struct_CProducer
	listenerFuncsMap sync.Map
}

func (p *defaultTransactionProducer) String() string {
	return p.config.String()
}

// Start the producer.
func (p *defaultTransactionProducer) Start() error {
	err := rmqError(C.StartProducer(p.cproduer))
	if err != NIL {
		return err
	}
	return nil
}

// Shutdown the producer.
func (p *defaultTransactionProducer) Shutdown() error {
	err := rmqError(C.ShutdownProducer(p.cproduer))

	if err != NIL {
		return err
	}

	err = rmqError(int(C.DestroyProducer(p.cproduer)))
	if err != NIL {
		return err
	}

	return err
}

func (p *defaultTransactionProducer) SendMessageTransaction(msg *Message, arg interface{}) (*SendResult, error) {
	cmsg := goMsgToC(msg)
	defer C.DestroyMessage(cmsg)

	var sr C.struct__SendResult_
	err := rmqError(C.SendMessageTransaction(p.cproduer, cmsg, (C.CLocalTransactionExecutorCallback)(unsafe.Pointer(C.transactionExecutor_cgo)), unsafe.Pointer(&arg), &sr))
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
