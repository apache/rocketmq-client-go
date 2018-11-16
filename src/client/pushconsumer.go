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
//#include "CMessageExt.h"
//#include "CPushConsumer.h"
//extern int ConsumeMessageCallback(CPushConsumer *consumer,CMessageExt *msg);
//int ConsumerMessageCallBackInner(CPushConsumer *consumer, CMessageExt *msg) {
//return ConsumeMessageCallback(consumer,msg);
//}
//int SetConsumerMessageCallBackInner(CPushConsumer *consumer) {
//return RegisterMessageCallback(consumer,ConsumerMessageCallBackInner);
//}
import "C"
import "fmt"

//type PushConsumer C.struct_CPushConsumer
//type MessageExt C.struct_CMessageExt
type Callback func(msg MessageExt) ConsumeStatus

func CreatePushConsumer(groupId string) (consumer PushConsumer) {
	consumer = C.CreatePushConsumer(C.CString(groupId))
	return consumer;
}
func DestroyPushConsumer(consumer PushConsumer) {
	consumer = C.DestroyPushConsumer(consumer.(*C.struct_CPushConsumer))
	return
}
func StartPushConsumer(consumer PushConsumer) int {
	return int(C.StartPushConsumer(consumer.(*C.struct_CPushConsumer)))
}
func ShutdownPushConsumer(consumer PushConsumer) int {
	return int(C.ShutdownPushConsumer(consumer.(*C.struct_CPushConsumer)))
}
func SetPushConsumerGroupID(consumer PushConsumer, groupId string) (int) {
	return int(C.SetPushConsumerGroupID(consumer.(*C.struct_CPushConsumer), C.CString(groupId)))
}
func SetPushConsumerNameServerAddress(consumer PushConsumer, name string) (int) {
	return int(C.SetPushConsumerNameServerAddress(consumer.(*C.struct_CPushConsumer), C.CString(name)))
}
func SetPushConsumerThreadCount(consumer PushConsumer, count int) (int) {
	return int(C.SetPushConsumerThreadCount(consumer.(*C.struct_CPushConsumer), C.int(count)))
}
func SetPushConsumerMessageBatchMaxSize(consumer PushConsumer, size int) (int) {
	return int(C.SetPushConsumerMessageBatchMaxSize(consumer.(*C.struct_CPushConsumer), C.int(size)))
}
func SetPushConsumerInstanceName(consumer PushConsumer, name string) (int) {
	return int(C.SetPushConsumerInstanceName(consumer.(*C.struct_CPushConsumer), C.CString(name)))
}
func SetPushConsumerSessionCredentials(consumer PushConsumer, accessKey string, secretKey string, channel string) (int) {
	ret := C.SetPushConsumerSessionCredentials(consumer.(*C.struct_CPushConsumer),
		C.CString(accessKey),
		C.CString(secretKey),
		C.CString(channel))
	return int(ret)
}

func Subscribe(consumer PushConsumer, topic string, expression string) (int) {
	return int(C.Subscribe(consumer.(*C.struct_CPushConsumer), C.CString(topic), C.CString(expression)))
}

var CallBackMap map[PushConsumer]Callback = map[PushConsumer]Callback{}

func RegisterMessageCallback(consumer PushConsumer, callback Callback) (int) {
	CallBackMap[consumer] = callback
	ret := C.SetConsumerMessageCallBackInner(consumer.(*C.struct_CPushConsumer))
	return int(ret)
}
func ConsumeMessageInner(consumer PushConsumer, msg MessageExt) (ConsumeStatus) {
	fmt.Println("ConsumeMessageInner")
	callback,ok := CallBackMap[consumer]
	if ok {
		return callback(msg)
	}
	return ReConsumeLater
}
