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
package client

//#cgo CFLAGS: -I/usr/local/include/rocketmq
//#cgo LDFLAGS: -L/usr/local/lib -lrocketmq
//typedef struct CMessage CMessage;
//typedef struct CProducer CProducer;
//#include "CMessage.h"
//#include "CProducer.h"
import "C"

//type Producer C.struct_CProducer
//type Message C.struct_CMessage
//type SendResult C.struct_CSendResult
//type PushConsumer C.struct_CPushConsumer
//type MessageExt C.struct_CMessageExt

func CreateMessage(topic string)(msg Message){
    msg = C.CreateMessage(C.CString(topic))
    return msg;
}
func DestroyMessage(msg Message){
    C.DestroyMessage(msg.(*C.struct_CMessage))
}
func SetMessageKeys(msg Message,keys string)(int){
    return int(C.SetMessageKeys(msg.(*C.struct_CMessage),C.CString(keys)))
}
func SetMessageBody(msg Message,body string)(int){
	return int(C.SetMessageBody(msg.(*C.struct_CMessage),C.CString(body)))
}
func CreateProducer(groupId string)(producer Producer){
    producer = C.CreateProducer(C.CString(groupId))
    return producer;
}
func DestroyProducer(producer Producer){
	C.DestroyProducer(producer.(*C.struct_CProducer))
}
func StartProducer(producer Producer)(int){
	return int(C.StartProducer(producer.(*C.struct_CProducer)))
}
func ShutdownProducer(producer Producer)(int){
	return int(C.ShutdownProducer(producer.(*C.struct_CProducer)))
}
func SetProducerNameServerAddress(producer Producer, nameServer string)(int){
	return int(C.SetProducerNameServerAddress(producer.(*C.struct_CProducer),C.CString(nameServer)))
}
func SetProducerSessionCredentials(producer Producer, accessKey string, secretKey string, channel string) (int) {
	ret := C.SetProducerSessionCredentials(producer.(*C.struct_CProducer),
		C.CString(accessKey),
		C.CString(secretKey),
		C.CString(channel))
	return int(ret)
}
func SendMessageSync(producer Producer, msg Message)(sendResult SendResult){
	var sr C.struct__SendResult_
	C.SendMessageSync(producer.(*C.struct_CProducer),msg.(*C.struct_CMessage),&sr)
	sendResult.Status = SendStatus(sr.sendStatus)
	sendResult.MsgId = C.GoString(&sr.msgId[0])
	sendResult.Offset = int64(sr.offset)
	return sendResult
}