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

//#cgo LDFLAGS: -L/usr/local/lib/ -lrocketmq
//#include "rocketmq/CMessage.h"
//#include "rocketmq/CProducer.h"
//#include "rocketmq/CSendResult.h"
import "C"
import "fmt"

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

func newDefaultProducer(config *ProducerConfig) *defaultProducer {
	producer := &defaultProducer{config: config}
	producer.cproduer = C.CreateProducer(C.CString(config.GroupID))
	code := int(C.SetProducerNameServerAddress(producer.cproduer, C.CString(producer.config.NameServer)))
	if config.Credentials != nil {
		ret := C.SetProducerSessionCredentials(producer.cproduer,
			C.CString(config.Credentials.AccessKey),
			C.CString(config.Credentials.SecretKey),
			C.CString(config.Credentials.Channel))
		code = int(ret)
	}
	switch code {

	}
	return producer
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
	err := int(C.StartProducer(p.cproduer))
	// TODO How to process err code.
	fmt.Printf("result: %v \n", err)
	return nil
}

// Shutdown the producer.
func (p *defaultProducer) Shutdown() error {
	defer C.DestroyProducer(p.cproduer)
	err := C.ShutdownProducer(p.cproduer)

	// TODO How to process err code.
	fmt.Printf("result: %v \n", err)
	return nil
}

func (p *defaultProducer) SendMessageSync(msg *Message) SendResult {
	cmsg := goMsgToC(msg)
	defer C.DestroyMessage(cmsg)

	var sr C.struct__SendResult_
	C.SendMessageSync(p.cproduer, cmsg, &sr)

	result := SendResult{}
	result.Status = SendStatus(sr.sendStatus)
	result.MsgId = C.GoString(&sr.msgId[0])
	result.Offset = int64(sr.offset)
	return result
}

func (p *defaultProducer) SendMessageAsync(msg *Message) {
	// TODO
}
